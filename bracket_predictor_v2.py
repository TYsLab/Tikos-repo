#!/usr/bin/env python3
"""
NCAA March Madness 2026 — Advanced Bracket Predictor v2

Data sources:
  - ESPN free API     : recent results, win/loss, basic stats
  - X/Twitter API     : real-time injury news, rivalry mentions
  - Gemini API        : scout reports, efficiency ratings, coaching data,
                        historical patterns, rivalry/feud analysis

Factors modeled:
  Team Performance   : off/def efficiency, tempo, SOS, recent form
  Matchup-Specific   : tempo clash, 3pt shooting vs defense,
                       turnover/steal matchup, rebounding margin
  Human Factors      : injuries, coaching experience, travel, rest days
  Historical         : mid-major patterns, 12-seed curse, conf. tendencies
  Rivalry/Feuds      : adds variance to games between traditional rivals

Usage:
  python3 bracket_predictor_v2.py
"""

import os
import json
import math
import random
import time
from dataclasses import dataclass, field
from datetime import datetime
from pathlib import Path
from typing import Optional

try:
    import requests
    REQUESTS_OK = True
except ImportError:
    REQUESTS_OK = False

try:
    from dotenv import load_dotenv
    load_dotenv(Path(__file__).parent / ".env", override=True)
except ImportError:
    pass

BASE_DIR      = Path(__file__).parent
RESULTS_PATH  = BASE_DIR / "bracket_results.json"
PROFILES_PATH = BASE_DIR / "team_profiles.json"   # cache scouted data

GEMINI_KEY    = os.getenv("GEMINI_API_KEY", "")
X_TOKEN       = os.getenv("X_BEARER_TOKEN", "")

# ── Bracket seed matchup order ────────────────────────────────────────────────

REGION_MATCHUP_ORDER = [(1,16),(8,9),(5,12),(4,13),(6,11),(3,14),(7,10),(2,15)]

# Historical first-round upset rates (baseline still anchors the model)
FIRST_ROUND_UPSET_RATE = {
    (1,16): 0.01, (2,15): 0.06, (3,14): 0.15, (4,13): 0.21,
    (5,12): 0.36, (6,11): 0.38, (7,10): 0.40, (8,9):  0.49,
}

# Seeds historically known to over/underperform
SEED_BIAS = {
    12: +0.04,   # 12-seeds beat 5-seeds more than expected
    11: +0.03,   # 11-seeds are dangerous
    10: +0.02,
    5:  -0.03,   # 5-seeds under-perform vs 12-seeds
    1:  +0.01,   # 1-seeds slightly over-perform historical rates
}

# Conference tournament toughness multiplier (tougher conf = better SOS proxy)
CONF_SOS_BOOST = {
    "SEC": 0.10, "Big 12": 0.10, "ACC": 0.09, "Big Ten": 0.09,
    "Big East": 0.07, "Pac-12": 0.05, "American": 0.02,
}

# ── Fallback 2026 teams (updated when ESPN bracket goes live) ─────────────────

FALLBACK_TEAMS = {
    # Source: 2026 NCAA Tournament official bracket, Selection Sunday March 15 2026
    "East": [
        (1,"Duke Blue Devils"),(2,"Connecticut Huskies"),(3,"Michigan State Spartans"),
        (4,"Kansas Jayhawks"),(5,"St. John's Red Storm"),(6,"Louisville Cardinals"),
        (7,"UCLA Bruins"),(8,"Ohio State Buckeyes"),(9,"TCU Horned Frogs"),
        (10,"UCF Knights"),(11,"South Florida Bulls"),(12,"Northern Iowa Panthers"),
        (13,"Cal Baptist Lancers"),(14,"North Dakota State Bison"),
        (15,"Furman Paladins"),(16,"Siena Saints"),
    ],
    "South": [
        (1,"Florida Gators"),(2,"Houston Cougars"),(3,"Illinois Fighting Illini"),
        (4,"Nebraska Cornhuskers"),(5,"Vanderbilt Commodores"),(6,"North Carolina Tar Heels"),
        (7,"Saint Mary's Gaels"),(8,"Clemson Tigers"),(9,"Iowa Hawkeyes"),
        (10,"Texas A&M Aggies"),(11,"VCU Rams"),(12,"McNeese Cowboys"),
        (13,"Troy Trojans"),(14,"Penn Quakers"),
        (15,"Idaho Vandals"),(16,"Prairie View A&M Panthers"),
    ],
    "West": [
        (1,"Arizona Wildcats"),(2,"Purdue Boilermakers"),(3,"Gonzaga Bulldogs"),
        (4,"Arkansas Razorbacks"),(5,"Wisconsin Badgers"),(6,"BYU Cougars"),
        (7,"Miami Hurricanes"),(8,"Villanova Wildcats"),(9,"Utah State Aggies"),
        (10,"Missouri Tigers"),(11,"NC State Wolfpack"),(12,"High Point Panthers"),
        (13,"Hawaii Warriors"),(14,"Kennesaw State Owls"),
        (15,"Queens Royals"),(16,"LIU Sharks"),
    ],
    "Midwest": [
        (1,"Michigan Wolverines"),(2,"Iowa State Cyclones"),(3,"Virginia Cavaliers"),
        (4,"Alabama Crimson Tide"),(5,"Texas Tech Red Raiders"),(6,"Tennessee Volunteers"),
        (7,"Kentucky Wildcats"),(8,"Dayton Flyers"),(9,"Baylor Bears"),
        (10,"Santa Clara Broncos"),(11,"SMU Mustangs"),(12,"Akron Zips"),
        (13,"Hofstra Pride"),(14,"Wright State Raiders"),
        (15,"Tennessee State Tigers"),(16,"Howard Bison"),
    ],
}

# ── Team Profile dataclass ────────────────────────────────────────────────────

@dataclass
class TeamProfile:
    name:               str
    seed:               int
    region:             str
    # Team Performance
    off_efficiency:     float = 105.0   # points per 100 possessions (adj.)
    def_efficiency:     float = 105.0   # lower = better defense
    tempo:              float = 68.0    # possessions per game
    sos:                float = 0.5     # strength of schedule 0-1
    recent_form:        float = 0.6     # win rate last 10 games (0-1)
    conf_tourney_perf:  float = 0.5     # conf tourney performance 0-1
    # Matchup factors
    three_pct:          float = 0.34    # team 3pt shooting %
    three_def:          float = 0.34    # opponent 3pt % allowed
    tov_rate:           float = 16.0    # turnovers per 100 possessions
    steal_rate:         float = 8.0     # steals per 100 possessions
    reb_margin:         float = 0.0     # rebounding margin per game
    # Human factors
    injury_impact:      float = 0.0     # 0=healthy, 1=crippling injury
    coaching_wins:      int   = 10      # head coach career NCAA tourney wins
    travel_burden:      float = 0.0     # 0=home region, 1=far travel
    rest_days:          int   = 2       # days rest before game
    # Historical / pattern
    is_mid_major:       bool  = False
    conf_name:          str   = "Unknown"
    historical_note:    str   = ""
    # Rivalry
    rivals:             list  = field(default_factory=list)
    # Raw Gemini scout summary
    scout_summary:      str   = ""


# ── ESPN free API ─────────────────────────────────────────────────────────────

def fetch_espn_stats(team_name: str) -> dict:
    """
    Try to pull basic stats from ESPN. Returns partial dict on failure.
    """
    if not REQUESTS_OK:
        return {}
    headers = {"User-Agent": "BracketPredictor/2.0"}
    # Search for team ID
    try:
        url = "https://site.api.espn.com/apis/site/v2/sports/basketball/mens-college-basketball/teams"
        r = requests.get(url, params={"limit": 500}, headers=headers, timeout=8)
        if r.status_code != 200:
            return {}
        all_teams = r.json().get("sports", [{}])[0].get("leagues", [{}])[0].get("teams", [])
        team_id = None
        for t in all_teams:
            display = t.get("team", {}).get("displayName", "")
            short = t.get("team", {}).get("shortDisplayName", "")
            if team_name.lower() in display.lower() or display.lower() in team_name.lower():
                team_id = t["team"]["id"]
                break
        if not team_id:
            return {}

        # Fetch team stats
        stats_url = f"https://site.api.espn.com/apis/site/v2/sports/basketball/mens-college-basketball/teams/{team_id}/statistics"
        sr = requests.get(stats_url, headers=headers, timeout=8)
        if sr.status_code != 200:
            return {}
        stats = sr.json().get("splits", {}).get("categories", [])
        out = {}
        for cat in stats:
            for stat in cat.get("stats", []):
                out[stat.get("name", "")] = stat.get("value", 0)
        return out
    except Exception:
        return {}


def fetch_espn_recent_games(team_name: str, n: int = 10) -> list:
    """Return last N game results as list of {'win': bool, 'opponent': str}"""
    # We get this from the Gemini scout since ESPN game-by-game is complex to parse
    return []


# ── X/Twitter injury scanner ──────────────────────────────────────────────────

def fetch_injury_news(team_names: list) -> dict:
    """
    Search X for injury news for each team. Returns {team_name: [tweet_text, ...]}.
    """
    if not REQUESTS_OK or not X_TOKEN:
        print("  X/Twitter: no token, skipping injury scan.")
        return {}

    headers = {"Authorization": f"Bearer {X_TOKEN}"}
    results = {}
    queries = []

    # Batch teams into a few queries to save rate limit
    chunk_size = 8
    for i in range(0, len(team_names), chunk_size):
        chunk = team_names[i:i+chunk_size]
        # Search for injury + any of these team names
        team_part = " OR ".join(f'"{n.split()[0]} {n.split()[1] if len(n.split())>1 else ""}"'.strip() for n in chunk[:4])
        queries.append((chunk, f"({team_part}) (injury OR injured OR out OR doubtful OR questionable) lang:en -is:retweet"))

    for teams_in_query, query in queries:
        try:
            r = requests.get(
                "https://api.twitter.com/2/tweets/search/recent",
                headers=headers,
                params={"query": query, "max_results": 20, "tweet.fields": "text,created_at"},
                timeout=10,
            )
            if r.status_code == 429:
                print("  X rate limit hit — using partial injury data.")
                break
            if r.status_code != 200:
                continue
            tweets = r.json().get("data", [])
            for tweet in tweets:
                text = tweet.get("text", "")
                # Assign tweet to best matching team
                for team in teams_in_query:
                    first_word = team.split()[0].lower()
                    if first_word in text.lower():
                        results.setdefault(team, []).append(text[:200])
                        break
            time.sleep(0.5)
        except Exception as e:
            print(f"  X query error: {e}")

    return results


# ── Gemini scout reporter ─────────────────────────────────────────────────────

SCOUT_PROMPT = """You are an expert NCAA basketball analyst preparing scouting reports for the 2026 March Madness tournament.

For EACH team listed below, return a JSON object with these fields:
- off_efficiency: float 88-125 (offensive points per 100 possessions, adjusted for schedule)
- def_efficiency: float 88-115 (defensive points allowed per 100 possessions — LOWER is better defense)
- tempo: float 60-80 (avg possessions per game — lower = slower/deliberate style)
- sos: float 0.0-1.0 (strength of schedule — 1.0 = toughest)
- recent_form: float 0.0-1.0 (last 10 game win rate weighted by recency, including conference tournament)
- conf_tourney_perf: float 0.0-1.0 (how well they performed in conference tournament: 1=champion, 0.5=average, 0=early exit)
- three_pct: float 0.25-0.45 (team 3-point shooting percentage)
- three_def: float 0.28-0.42 (opponent 3-point percentage they allow)
- tov_rate: float 10-25 (turnovers per 100 possessions — lower = better ball security)
- steal_rate: float 5-15 (steals per 100 possessions)
- reb_margin: float -8 to +12 (rebounding margin per game)
- injury_impact: float 0.0-0.8 (0=fully healthy, 0.8=star player out)
- injury_details: string (brief note on any injuries, or "Healthy")
- coaching_wins: int (head coach career NCAA tournament wins — e.g. Coach K had 100+, John Calipari ~70)
- is_mid_major: bool (true if not from a Power 5 / major conference)
- conf_name: string (conference name)
- travel_burden: float 0.0-0.5 (0=plays near home region, 0.5=must travel across country)
- historical_note: string (1-2 sentences about this team's recent tournament history and tendencies)
- rivals: list of strings (names of other teams IN THIS TOURNAMENT that are traditional rivals or have recent feuds/beef — player rivalries count too)
- rivalry_notes: string (brief note on any relevant feuds or rivalries with tournament opponents)
- scout_summary: string (3-4 sentence scouting report covering style of play, strengths, weaknesses, and tournament outlook)

Also flag any KNOWN RIVALRIES or FEUDS between players from different teams currently in this tournament — this creates extra emotional variance.

Teams to scout:
{team_list}

Return a JSON object where each key is the EXACT team name from the list above.
Be precise with the numbers — base them on the 2025-26 season stats.
Return only valid JSON, no markdown fences."""


def fetch_gemini_scout_reports(teams: list) -> dict:
    """
    Call Gemini to get scout reports for all teams at once.
    Returns {team_name: {stats_dict}} or {} on failure.
    """
    if not REQUESTS_OK or not GEMINI_KEY:
        print("  Gemini: no API key, using defaults.")
        return {}

    team_list = "\n".join(f"- {t['name']} (Seed {t['seed']}, {t['region']} region)" for t in teams)
    prompt = SCOUT_PROMPT.replace("{team_list}", team_list)

    print(f"  Calling Gemini for {len(teams)} team scout reports...")
    try:
        r = requests.post(
            f"https://generativelanguage.googleapis.com/v1beta/models/gemini-2.5-flash:generateContent?key={GEMINI_KEY}",
            json={
                "contents": [{"parts": [{"text": prompt}]}],
                "generationConfig": {
                    "responseMimeType": "application/json",
                    "temperature": 0.3,
                }
            },
            headers={"Content-Type": "application/json"},
            timeout=120,
        )
        if r.status_code != 200:
            print(f"  Gemini error {r.status_code}: {r.text[:200]}")
            return {}

        raw = r.json()
        text = raw["candidates"][0]["content"]["parts"][0]["text"].strip()
        if text.startswith("```"):
            lines = text.split("\n")
            text = "\n".join(lines[1:-1])
        data = json.loads(text)
        print(f"  Gemini returned data for {len(data)} teams.")
        return data

    except json.JSONDecodeError as e:
        print(f"  Gemini JSON parse error: {e}")
        return {}
    except Exception as e:
        print(f"  Gemini fetch error: {e}")
        return {}


# ── Build TeamProfile from all sources ───────────────────────────────────────

def build_profiles(teams: list, scout_data: dict, injury_data: dict) -> dict:
    """
    Merge team list + Gemini scout data + X injury data into TeamProfile objects.
    Returns {team_name: TeamProfile}
    """
    profiles = {}

    for team in teams:
        name = team["name"]
        seed = team["seed"]
        region = team["region"]
        scout = scout_data.get(name, {})

        # Parse X injury tweets — if they mention "out" or "injured" bump impact
        x_tweets = injury_data.get(name, [])
        x_injury_bump = 0.0
        for tweet in x_tweets:
            lower = tweet.lower()
            if any(w in lower for w in ["out for", "ruled out", "season-ending", "torn", "fracture"]):
                x_injury_bump = max(x_injury_bump, 0.5)
            elif any(w in lower for w in ["doubtful", "won't play", "will not play"]):
                x_injury_bump = max(x_injury_bump, 0.35)
            elif any(w in lower for w in ["questionable", "limited", "day-to-day"]):
                x_injury_bump = max(x_injury_bump, 0.15)

        gemini_injury = float(scout.get("injury_impact", 0.0))
        final_injury  = max(gemini_injury, x_injury_bump)

        # Conference SOS boost
        conf = scout.get("conf_name", "Unknown")
        conf_boost = CONF_SOS_BOOST.get(conf, 0.0)
        sos_base = float(scout.get("sos", 0.5))
        sos_final = min(1.0, sos_base + conf_boost)

        # Historical seed bias nudge to recent_form
        form_base = float(scout.get("recent_form", 0.6))

        p = TeamProfile(
            name=name, seed=seed, region=region,
            off_efficiency  = float(scout.get("off_efficiency",  105.0)),
            def_efficiency  = float(scout.get("def_efficiency",  105.0)),
            tempo           = float(scout.get("tempo",           68.0)),
            sos             = sos_final,
            recent_form     = form_base,
            conf_tourney_perf = float(scout.get("conf_tourney_perf", 0.5)),
            three_pct       = float(scout.get("three_pct",       0.34)),
            three_def       = float(scout.get("three_def",       0.34)),
            tov_rate        = float(scout.get("tov_rate",        16.0)),
            steal_rate      = float(scout.get("steal_rate",      8.0)),
            reb_margin      = float(scout.get("reb_margin",      0.0)),
            injury_impact   = final_injury,
            coaching_wins   = int(scout.get("coaching_wins",     10)),
            travel_burden   = float(scout.get("travel_burden",   0.1)),
            rest_days       = 2,
            is_mid_major    = bool(scout.get("is_mid_major",     seed >= 11)),
            conf_name       = conf,
            historical_note = str(scout.get("historical_note",   "")),
            rivals          = list(scout.get("rivals",           [])),
            scout_summary   = str(scout.get("scout_summary",     "")),
        )
        profiles[name] = p

    return profiles


# ── Win probability engine ────────────────────────────────────────────────────

def sigmoid(x: float) -> float:
    return 1.0 / (1.0 + math.exp(-x))

def log5(pa: float, pb: float) -> float:
    denom = pa + pb - 2 * pa * pb
    return (pa - pa * pb) / denom if abs(denom) > 1e-9 else 0.5

def seed_strength(seed: int) -> float:
    return 1.0 - (seed - 1) / 16 * 0.70


def matchup_win_prob(a: TeamProfile, b: TeamProfile,
                     noise_std: float = 0.0,
                     round_idx: int = 0) -> float:
    """
    Compute probability that team A beats team B.
    Blends advanced stats (70%) with seed baseline (30%).
    """
    score = 0.0

    # ── 1. Efficiency margin (most predictive factor) ──────────────────
    # Net rating differential: (a_off - b_def) vs (b_off - a_def)
    a_net = a.off_efficiency - b.def_efficiency
    b_net = b.off_efficiency - a.def_efficiency
    eff_adv = (a_net - b_net) / 20.0   # normalize ~[-1, 1]
    score += eff_adv * 0.35

    # ── 2. Recent form ──────────────────────────────────────────────────
    form_adv = (a.recent_form - b.recent_form) * 0.20
    score += form_adv

    # ── 3. Conference tournament performance ───────────────────────────
    conf_adv = (a.conf_tourney_perf - b.conf_tourney_perf) * 0.08
    score += conf_adv

    # ── 4. Strength of schedule ─────────────────────────────────────────
    sos_adv = (a.sos - b.sos) * 0.06
    score += sos_adv

    # ── 5. 3-point shooting matchup ─────────────────────────────────────
    # A's shooting advantage vs B's 3pt defense
    three_adv = (a.three_pct - b.three_def) - (b.three_pct - a.three_def)
    score += three_adv * 0.40   # ~4pts per 10% differential

    # ── 6. Tempo matchup ────────────────────────────────────────────────
    # In a tempo mismatch, slower team tends to benefit (controls pace)
    tempo_diff = a.tempo - b.tempo
    # Negative tempo_diff means A is slower — slight edge to A
    score -= tempo_diff * 0.003

    # ── 7. Turnover/steal matchup ───────────────────────────────────────
    # A benefits if B turns it over more AND A steals more
    tov_adv = (b.tov_rate - a.steal_rate) - (a.tov_rate - b.steal_rate)
    score += tov_adv * 0.015

    # ── 8. Rebounding margin ─────────────────────────────────────────────
    reb_adv = (a.reb_margin - b.reb_margin) / 15.0 * 0.10
    score += reb_adv

    # ── 9. Coaching tournament experience ───────────────────────────────
    # Experienced coaches over-perform in tournament, especially late rounds
    coach_diff = min(a.coaching_wins - b.coaching_wins, 40) / 40.0
    round_weight = 0.03 + round_idx * 0.01   # more important in later rounds
    score += coach_diff * round_weight

    # ── 10. Injury impact ───────────────────────────────────────────────
    injury_adv = b.injury_impact - a.injury_impact
    score += injury_adv * 0.30

    # ── 11. Travel / fatigue ─────────────────────────────────────────────
    travel_adv = (b.travel_burden - a.travel_burden) * 0.05
    score += travel_adv

    # ── 12. Rest days ────────────────────────────────────────────────────
    rest_adv = (a.rest_days - b.rest_days) / 3.0 * 0.03
    score += rest_adv

    # ── 13. Historical seed bias ─────────────────────────────────────────
    seed_bias_a = SEED_BIAS.get(a.seed, 0.0)
    seed_bias_b = SEED_BIAS.get(b.seed, 0.0)
    score += (seed_bias_a - seed_bias_b) * 0.5

    # ── 14. Mid-major penalty/bonus ──────────────────────────────────────
    if a.is_mid_major and not b.is_mid_major and a.seed > 8:
        score -= 0.04   # mid-majors face tougher path
    elif a.is_mid_major and not b.is_mid_major and a.seed <= 8:
        score += 0.02   # top mid-majors are underrated

    # ── 15. Rivalry / feud factor ────────────────────────────────────────
    # Rivalries add variance — outcome becomes more unpredictable
    rivalry_noise = 0.0
    if b.name in a.rivals or a.name in b.rivals:
        rivalry_noise = random.gauss(0, 0.08)   # high variance, can swing either way
    score += rivalry_noise

    # ── Blend with seed baseline ─────────────────────────────────────────
    # Earlier rounds lean more on seed, later rounds lean on stats
    seed_weight = max(0.15, 0.35 - round_idx * 0.05)
    stat_weight = 1.0 - seed_weight

    seed_prob = log5(seed_strength(a.seed), seed_strength(b.seed))
    stat_prob = sigmoid(score * 3.0)

    prob = stat_weight * stat_prob + seed_weight * seed_prob

    # Monte Carlo noise
    if noise_std > 0:
        prob += random.gauss(0, noise_std)

    return max(0.04, min(0.96, prob))


# ── Bracket simulation ────────────────────────────────────────────────────────

def _order_region_teams(region_teams: list) -> list:
    by_seed = {t["seed"]: t for t in region_teams}
    ordered = []
    for s1, s2 in REGION_MATCHUP_ORDER:
        t1, t2 = by_seed.get(s1), by_seed.get(s2)
        if t1: ordered.append(t1)
        if t2: ordered.append(t2)
    return ordered


def _play_round(teams: list, profiles: dict, noise: float, round_idx: int) -> list:
    winners = []
    for i in range(0, len(teams), 2):
        if i + 1 >= len(teams):
            winners.append(teams[i]); continue
        ta, tb = teams[i], teams[i+1]
        pa = profiles.get(ta["name"])
        pb = profiles.get(tb["name"])
        if pa and pb:
            prob = matchup_win_prob(pa, pb, noise, round_idx)
        else:
            # Fallback to seed if no profile
            prob = log5(seed_strength(ta["seed"]), seed_strength(tb["seed"]))
            if noise > 0:
                prob = max(0.04, min(0.96, prob + random.gauss(0, noise)))
        winner = ta if random.random() < prob else tb
        winners.append(winner)
    return winners


def simulate_bracket(teams: list, profiles: dict, algorithm: str) -> dict:
    noise = 0.06 if algorithm == "MonteCarlo" else 0.0
    # UpsetModel: temporarily boost underdog profiles
    if algorithm == "UpsetModel":
        for p in profiles.values():
            if 10 <= p.seed <= 15:
                p.recent_form = min(1.0, p.recent_form + 0.08)
                p.sos = min(1.0, p.sos + 0.05)

    regions_map = {}
    for t in teams:
        regions_map.setdefault(t["region"], []).append(t)

    region_winners = {}
    all_rounds = {}

    for region, rteams in regions_map.items():
        ordered = _order_region_teams(rteams)
        rounds_list = [ordered]
        current = ordered
        ri = 0
        while len(current) > 1:
            current = _play_round(current, profiles, noise, ri)
            rounds_list.append(current)
            ri += 1
        region_winners[region] = current[0]
        all_rounds[region] = rounds_list

    # Restore UpsetModel boosts
    if algorithm == "UpsetModel":
        for p in profiles.values():
            if 10 <= p.seed <= 15:
                p.recent_form = max(0.0, p.recent_form - 0.08)
                p.sos = max(0.0, p.sos - 0.05)

    # Final Four
    region_order = ["South", "East", "West", "Midwest"]
    available = {r: region_winners[r] for r in region_order if r in region_winners}
    region_list = list(available.keys())

    final_four = [available[r] for r in region_list]
    all_rounds["Final Four"] = [final_four]

    semi_pairs = [(region_list[0], region_list[1]), (region_list[2], region_list[3])] if len(region_list) >= 4 else []
    finalists = []
    for ra, rb in semi_pairs:
        ta, tb = available[ra], available[rb]
        prob = matchup_win_prob(profiles.get(ta["name"], _dummy(ta)),
                               profiles.get(tb["name"], _dummy(tb)), noise, 4)
        finalists.append(ta if random.random() < prob else tb)

    all_rounds["Final Four"].append(finalists)
    all_rounds["Championship"] = [finalists]

    champion = None
    if len(finalists) == 2:
        ta, tb = finalists[0], finalists[1]
        prob = matchup_win_prob(profiles.get(ta["name"], _dummy(ta)),
                               profiles.get(tb["name"], _dummy(tb)), noise, 5)
        champion = ta if random.random() < prob else tb
        all_rounds["Championship"].append([champion])
    elif finalists:
        champion = finalists[0]

    return {"algorithm": algorithm, "rounds": all_rounds,
            "final_four": final_four, "champion": champion}


def _dummy(team: dict) -> TeamProfile:
    return TeamProfile(name=team["name"], seed=team["seed"], region=team["region"])


# ── Monte Carlo aggregation ───────────────────────────────────────────────────

def run_monte_carlo(teams: list, profiles: dict, n: int = 10_000) -> dict:
    reach = {t["name"]: [0] * 6 for t in teams}
    regions_map = {}
    for t in teams:
        regions_map.setdefault(t["region"], []).append(t)

    region_order = ["South", "East", "West", "Midwest"]

    for _ in range(n):
        region_winners = {}
        for region, rteams in regions_map.items():
            ordered = _order_region_teams(rteams)
            current = ordered
            ri = 0
            for t in current:
                reach[t["name"]][0] += 1
            while len(current) > 1:
                current = _play_round(current, profiles, 0.06, ri)
                ri += 1
                depth = min(ri, 5)
                for t in current:
                    reach[t["name"]][depth] += 1
            region_winners[region] = current[0]

        available = {r: region_winners[r] for r in region_order if r in region_winners}
        region_list = list(available.keys())
        if len(region_list) < 2:
            continue
        semi_pairs = [(region_list[0], region_list[1]), (region_list[2], region_list[3])] if len(region_list) >= 4 else [(region_list[0], region_list[1])]
        finalists = []
        for ra, rb in semi_pairs:
            ta, tb = available[ra], available[rb]
            prob = matchup_win_prob(profiles.get(ta["name"], _dummy(ta)),
                                   profiles.get(tb["name"], _dummy(tb)), 0.06, 4)
            w = ta if random.random() < prob else tb
            finalists.append(w)
            reach[w["name"]][4] += 1

        if len(finalists) == 2:
            ta, tb = finalists[0], finalists[1]
            prob = matchup_win_prob(profiles.get(ta["name"], _dummy(ta)),
                                   profiles.get(tb["name"], _dummy(tb)), 0.06, 5)
            champ = ta if random.random() < prob else tb
            reach[champ["name"]][5] += 1

    return {name: [round(c/n*100, 2) for c in counts] for name, counts in reach.items()}


# ── Upset pick detector ───────────────────────────────────────────────────────

def find_best_upset_picks(teams: list, profiles: dict, mc_pcts: dict) -> list:
    """Find seeds 10-15 with strong advanced stats despite low seed."""
    picks = []
    for team in teams:
        if not (10 <= team["seed"] <= 15):
            continue
        p = profiles.get(team["name"])
        if not p:
            continue
        # Score upset potential
        champ_pct = (mc_pcts.get(team["name"]) or [0]*6)[5]
        net_rating = p.off_efficiency - p.def_efficiency
        upset_score = (
            (net_rating / 20.0) * 0.4 +
            p.recent_form * 0.3 +
            p.sos * 0.2 +
            (1 - p.injury_impact) * 0.1
        )
        if upset_score > 0.4 or champ_pct > 5.0:
            # Find who they play in R64
            r = team["region"]
            opponent_seed = 17 - team["seed"]  # e.g. 12 plays 5
            picks.append({
                "winner": team["name"],
                "winner_seed": team["seed"],
                "loser": f"#{opponent_seed} seed ({r})",
                "round": 1,
                "upset_score": round(upset_score, 3),
                "champ_pct": champ_pct,
                "reason": p.scout_summary[:150] if p.scout_summary else "Strong advanced metrics"
            })
    picks.sort(key=lambda x: x["upset_score"], reverse=True)
    return picks[:6]


# ── Main runner ───────────────────────────────────────────────────────────────

def load_teams():
    # Try ESPN live bracket first
    if REQUESTS_OK:
        try:
            r = requests.get(
                "http://site.api.espn.com/apis/site/v2/sports/basketball/mens-college-basketball/tournaments",
                timeout=8
            )
            if r.status_code == 200:
                # parse live bracket — if it returns 64 teams use it
                pass
        except Exception:
            pass
    print("  Using 2026 bracket data (update teams once official bracket releases).")
    teams = []
    for region, entries in FALLBACK_TEAMS.items():
        for seed, name in entries:
            teams.append({"name": name, "seed": seed, "region": region})
    return teams


def run_predictions() -> dict:
    random.seed()   # non-reproducible for live use

    print("=" * 60)
    print("  NCAA 2026 Advanced Bracket Predictor v2")
    print("=" * 60)

    # 1. Load teams
    print("\n[1/5] Loading bracket...")
    teams = load_teams()
    print(f"  {len(teams)} teams across {len(set(t['region'] for t in teams))} regions.")

    # 2. Check for cached profiles (skip re-scouting if recent)
    profiles_cache = {}
    if PROFILES_PATH.exists():
        age_hours = (datetime.now().timestamp() - PROFILES_PATH.stat().st_mtime) / 3600
        if age_hours < 12:
            print(f"\n[2/5] Using cached scout data ({age_hours:.1f}h old)...")
            with open(PROFILES_PATH) as f:
                profiles_cache = json.load(f)

    if not profiles_cache:
        # 3. Fetch Gemini scout reports
        print("\n[2/5] Fetching Gemini scout reports...")
        scout_data = fetch_gemini_scout_reports(teams)

        # 4. Fetch X injury updates
        print("\n[3/5] Scanning X/Twitter for injury news...")
        team_names = [t["name"] for t in teams]
        injury_data = fetch_injury_news(team_names)
        injured_teams = [k for k, v in injury_data.items() if v]
        print(f"  Found injury mentions for: {', '.join(injured_teams) if injured_teams else 'none'}")

        # Build profiles
        print("\n[4/5] Building team profiles...")
        profile_objects = build_profiles(teams, scout_data, injury_data)

        # Cache profiles
        profiles_cache = {
            name: {
                "off_efficiency": p.off_efficiency, "def_efficiency": p.def_efficiency,
                "tempo": p.tempo, "sos": p.sos, "recent_form": p.recent_form,
                "conf_tourney_perf": p.conf_tourney_perf, "three_pct": p.three_pct,
                "three_def": p.three_def, "tov_rate": p.tov_rate, "steal_rate": p.steal_rate,
                "reb_margin": p.reb_margin, "injury_impact": p.injury_impact,
                "coaching_wins": p.coaching_wins, "travel_burden": p.travel_burden,
                "is_mid_major": p.is_mid_major, "conf_name": p.conf_name,
                "historical_note": p.historical_note, "rivals": p.rivals,
                "scout_summary": p.scout_summary,
            }
            for name, p in profile_objects.items()
        }
        with open(PROFILES_PATH, "w") as f:
            json.dump(profiles_cache, f, indent=2)
    else:
        print("\n[3/5] Skipping X scan (using cache).")
        print("\n[4/5] Rebuilding profiles from cache...")
        profile_objects = build_profiles(teams, profiles_cache, {})

    # 5. Simulate bracket
    print("\n[5/5] Simulating bracket (3 algorithms + 10k Monte Carlo)...")

    results = {
        "generated_at": datetime.now().isoformat(),
        "version": "v2-advanced",
        "teams": teams,
        "rounds": {},
        "champion": {},
        "final_four": {},
        "consensus": [],
        "best_upset_picks": [],
        "monte_carlo_win_pcts": {},
        "team_profiles": profiles_cache,
    }

    for algo in ("SeedModel", "UpsetModel"):
        print(f"  Running {algo}...")
        sim = simulate_bracket(teams, profile_objects, algo)
        results["rounds"][algo]     = _serialize_rounds(sim["rounds"])
        results["champion"][algo]   = sim["champion"]
        results["final_four"][algo] = sim["final_four"]

    print("  Running MonteCarlo representative sim...")
    mc_sim = simulate_bracket(teams, profile_objects, "MonteCarlo")
    results["rounds"]["MonteCarlo"]     = _serialize_rounds(mc_sim["rounds"])
    results["champion"]["MonteCarlo"]   = mc_sim["champion"]
    results["final_four"]["MonteCarlo"] = mc_sim["final_four"]

    print("  Running 10,000 Monte Carlo simulations...")
    mc_pcts = run_monte_carlo(teams, profile_objects, 10_000)
    results["monte_carlo_win_pcts"] = mc_pcts

    # Consensus Final Four (all 3 agree)
    ff_sets = [
        {t["name"] for t in results["final_four"]["SeedModel"]},
        {t["name"] for t in results["final_four"]["UpsetModel"]},
        {t["name"] for t in results["final_four"]["MonteCarlo"]},
    ]
    results["consensus"] = sorted(ff_sets[0] & ff_sets[1] & ff_sets[2])

    # Best upset picks
    results["best_upset_picks"] = find_best_upset_picks(teams, profile_objects, mc_pcts)

    # Save
    with open(RESULTS_PATH, "w") as f:
        json.dump(results, f, indent=2, default=str)
    print(f"\n  Saved to {RESULTS_PATH}")

    _print_summary(results, profile_objects)
    return results


def _serialize_rounds(rounds: dict) -> dict:
    out = {}
    for rnd, stages in rounds.items():
        out[rnd] = []
        for stage in stages:
            out[rnd].append([
                {"name": t["name"], "seed": t["seed"], "region": t["region"]}
                for t in stage
            ])
    return out


def _print_summary(results: dict, profiles: dict):
    w = 65
    print("\n" + "=" * w)
    print("  CHAMPION PREDICTIONS".center(w))
    print("=" * w)
    for algo in ("SeedModel", "UpsetModel", "MonteCarlo"):
        ch = results["champion"].get(algo, {})
        name = ch.get("name", "?") if isinstance(ch, dict) else str(ch)
        seed = ch.get("seed", "?") if isinstance(ch, dict) else "?"
        p = profiles.get(name)
        injury = f" ⚠ Injury:{p.injury_impact:.1f}" if p and p.injury_impact > 0.1 else ""
        print(f"  {algo:<15} → ({seed}) {name}{injury}")

    print(f"\n  Consensus FF: {', '.join(results['consensus']) or 'No consensus'}")

    print("\n  BEST UPSET PICKS:")
    for pick in results["best_upset_picks"][:4]:
        print(f"  • ({pick['winner_seed']}) {pick['winner']} — score {pick['upset_score']}")
        print(f"    {pick['reason'][:80]}")

    print("\n  MONTE CARLO CHAMPIONSHIP ODDS (top 8):")
    mc = results["monte_carlo_win_pcts"]
    top = sorted([(n, p[5]) for n, p in mc.items() if len(p)>5], key=lambda x: -x[1])[:8]
    for name, pct in top:
        bar = "█" * int(pct/2)
        print(f"  {name:<38} {pct:5.1f}%  {bar}")
    print("=" * w)


if __name__ == "__main__":
    run_predictions()
