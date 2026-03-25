#!/usr/bin/env python3
"""
NCAA March Madness 2026 Bracket Prediction Engine.

Three prediction algorithms:
  - SeedModel:   Historical seed matchup win rates + log5 formula
  - UpsetModel:  SeedModel with inflated upset probability for seeds 10-15
  - MonteCarlo:  10,000 simulations with gaussian noise

Usage:
  python3 bracket_predictor.py
"""

import json
import math
import random
import time
from datetime import datetime
from pathlib import Path

try:
    import requests
    REQUESTS_AVAILABLE = True
except ImportError:
    REQUESTS_AVAILABLE = False

BASE_DIR = Path(__file__).parent
RESULTS_PATH = BASE_DIR / "bracket_results.json"

# ── Round names ───────────────────────────────────────────────────────────────

ROUND_NAMES = [
    "Round of 64",
    "Round of 32",
    "Sweet Sixteen",
    "Elite Eight",
    "Final Four",
    "Championship",
]

# ── Historical first-round seed matchup win rates (higher seed = favorite) ───

FIRST_ROUND_WIN_RATES = {
    (1, 16): 0.99,
    (2, 15): 0.94,
    (3, 14): 0.85,
    (4, 13): 0.79,
    (5, 12): 0.64,
    (6, 11): 0.62,
    (7, 10): 0.60,
    (8,  9): 0.51,
}

# Standard bracket seeding matchup order within a region
REGION_MATCHUP_ORDER = [(1, 16), (8, 9), (5, 12), (4, 13), (6, 11), (3, 14), (7, 10), (2, 15)]

# ── Fallback 2026 bracket data ────────────────────────────────────────────────

FALLBACK_TEAMS = {
    "South": [
        (1,  "Duke Blue Devils"),
        (2,  "Alabama Crimson Tide"),
        (3,  "Wisconsin Badgers"),
        (4,  "Missouri Tigers"),
        (5,  "Memphis Tigers"),
        (6,  "BYU Cougars"),
        (7,  "Michigan State Spartans"),
        (8,  "Mississippi State Bulldogs"),
        (9,  "Boise State Broncos"),
        (10, "Utah State Aggies"),
        (11, "VCU Rams"),
        (12, "UC San Diego Tritons"),
        (13, "High Point Panthers"),
        (14, "Morehead State Eagles"),
        (15, "Lipscomb Bisons"),
        (16, "Norfolk State Spartans"),
    ],
    "East": [
        (1,  "Auburn Tigers"),
        (2,  "Michigan Wolverines"),
        (3,  "Iowa State Cyclones"),
        (4,  "Maryland Terrapins"),
        (5,  "Gonzaga Bulldogs"),
        (6,  "Clemson Tigers"),
        (7,  "Xavier Musketeers"),
        (8,  "Dayton Flyers"),
        (9,  "Oklahoma Sooners"),
        (10, "New Mexico Lobos"),
        (11, "Drake Bulldogs"),
        (12, "Colorado State Rams"),
        (13, "Colgate Raiders"),
        (14, "UNCW Seahawks"),
        (15, "Longwood Lancers"),
        (16, "Mount St. Mary's Mountaineers"),
    ],
    "West": [
        (1,  "Kansas Jayhawks"),
        (2,  "Tennessee Volunteers"),
        (3,  "Creighton Bluejays"),
        (4,  "Purdue Boilermakers"),
        (5,  "Marquette Golden Eagles"),
        (6,  "Illinois Fighting Illini"),
        (7,  "UCLA Bruins"),
        (8,  "Texas A&M Aggies"),
        (9,  "TCU Horned Frogs"),
        (10, "Penn State Nittany Lions"),
        (11, "Indiana Hoosiers"),
        (12, "McNeese Cowboys"),
        (13, "Furman Paladins"),
        (14, "Montana Grizzlies"),
        (15, "Bryant Bulldogs"),
        (16, "Texas Southern Tigers"),
    ],
    "Midwest": [
        (1,  "Houston Cougars"),
        (2,  "St. John's Red Storm"),
        (3,  "Kentucky Wildcats"),
        (4,  "Florida Gators"),
        (5,  "Oregon Ducks"),
        (6,  "Cincinnati Bearcats"),
        (7,  "Nebraska Cornhuskers"),
        (8,  "Vanderbilt Commodores"),
        (9,  "Georgia Bulldogs"),
        (10, "Arkansas Razorbacks"),
        (11, "San Diego State Aztecs"),
        (12, "Liberty Flames"),
        (13, "Akron Zips"),
        (14, "Vermont Catamounts"),
        (15, "Wofford Terriers"),
        (16, "Stetson Hatters"),
    ],
}

# ── Data fetching ─────────────────────────────────────────────────────────────

def _fetch_espn_bracket():
    """
    Attempt to fetch live 2026 bracket data from ESPN public API.
    Returns a list of team dicts or None on failure.
    """
    if not REQUESTS_AVAILABLE:
        return None

    endpoints = [
        "http://site.api.espn.com/apis/site/v2/sports/basketball/mens-college-basketball/tournaments",
        "https://site.api.espn.com/apis/site/v2/sports/basketball/mens-college-basketball/scoreboard?groups=100",
    ]

    for url in endpoints:
        try:
            resp = requests.get(url, timeout=8)
            if resp.status_code != 200:
                continue
            data = resp.json()

            # Try to parse teams out of the tournament endpoint
            teams = _parse_espn_tournament(data)
            if teams and len(teams) >= 64:
                print(f"  Loaded live bracket from ESPN: {url}")
                return teams[:64]
        except Exception as exc:
            print(f"  ESPN fetch failed ({url}): {exc}")
            continue

    return None


def _parse_espn_tournament(data):
    """
    Parse ESPN tournament JSON into a flat list of team dicts.
    ESPN's structure varies; we do a best-effort parse.
    """
    teams = []
    seen = set()

    def _walk(node):
        if not isinstance(node, (dict, list)):
            return
        if isinstance(node, list):
            for item in node:
                _walk(item)
            return
        # Look for competitor / team nodes
        if "competitors" in node:
            for comp in node["competitors"]:
                _extract_team(comp)
        for val in node.values():
            _walk(val)

    def _extract_team(comp):
        try:
            team_node = comp.get("team", comp)
            name = team_node.get("displayName") or team_node.get("name", "")
            seed_val = comp.get("seed") or team_node.get("seed")
            seed = int(seed_val) if seed_val else None
            # Region is typically in a parent node; we skip if missing
            if name and seed and name not in seen and 1 <= seed <= 16:
                seen.add(name)
                teams.append({"name": name, "seed": seed, "region": "Unknown"})
        except Exception:
            pass

    _walk(data)
    return teams if len(teams) >= 16 else None


def load_bracket():
    """
    Return 64 teams as a list of dicts: {name, seed, region}.
    Tries ESPN first; falls back to hardcoded 2026 data.
    """
    live = _fetch_espn_bracket()
    if live:
        return live

    print("  Using fallback 2026 bracket data.")
    teams = []
    for region, entries in FALLBACK_TEAMS.items():
        for seed, name in entries:
            teams.append({"name": name, "seed": seed, "region": region})
    return teams


# ── Strength / probability utilities ─────────────────────────────────────────

def seed_strength(seed: int) -> float:
    """Map seed 1-16 to a [0.30, 1.00] strength rating."""
    return 1.0 - (seed - 1) / 16 * 0.70


def log5(pa: float, pb: float) -> float:
    """Log5 formula: probability that team A beats team B given win rates pa, pb."""
    denom = pa + pb - 2 * pa * pb
    if abs(denom) < 1e-9:
        return 0.5
    return (pa - pa * pb) / denom


def matchup_win_prob(team_a: dict, team_b: dict, model: str = "seed") -> float:
    """
    Return probability that team_a beats team_b.

    model: "seed"   — pure seed-based
           "upset"  — seed-based with 15% inflation for seeds 10-15
    """
    sa, sb = team_a["seed"], team_b["seed"]
    lo_seed, hi_seed = min(sa, sb), max(sa, sb)

    # First-round lookup
    key = (lo_seed, hi_seed)
    if key in FIRST_ROUND_WIN_RATES:
        base_prob = FIRST_ROUND_WIN_RATES[key]
        # base_prob is always for the LOWER (better) seed
        prob_a_wins = base_prob if sa == lo_seed else (1 - base_prob)
    else:
        # Later rounds: use log5 with strength ratings
        str_a = seed_strength(sa)
        str_b = seed_strength(sb)
        prob_a_wins = log5(str_a, str_b)

    if model == "upset":
        # Inflate upset probability by 15% for high seeds (10-15)
        # "upset" means the worse seed (higher number) wins
        upset_seed = sa if sa > sb else sb  # the underdog
        if 10 <= upset_seed <= 15:
            # Whichever team IS the underdog gets +15% relative boost
            if sa == upset_seed:
                # team_a is the underdog; inflate their win chance
                inflation = 0.15 * (1 - prob_a_wins)
                prob_a_wins = min(0.95, prob_a_wins + inflation)
            else:
                # team_b is the underdog; deflate team_a's win chance
                inflation = 0.15 * prob_a_wins
                prob_a_wins = max(0.05, prob_a_wins - inflation)

    return prob_a_wins


# ── Core bracket simulator ────────────────────────────────────────────────────

def _order_region_teams(region_teams: list) -> list:
    """
    Return teams in standard bracket seed-pair order for a 16-team region.
    Pairs: (1,16), (8,9), (5,12), (4,13), (6,11), (3,14), (7,10), (2,15)
    """
    by_seed = {t["seed"]: t for t in region_teams}
    ordered = []
    for s1, s2 in REGION_MATCHUP_ORDER:
        t1 = by_seed.get(s1)
        t2 = by_seed.get(s2)
        if t1 and t2:
            ordered.extend([t1, t2])
        elif t1:
            ordered.append(t1)
        elif t2:
            ordered.append(t2)
    return ordered


def _play_round(bracket_half: list, model: str, noise_std: float = 0.0) -> list:
    """
    Simulate one round of play for a set of teams (must be even count).
    Returns winners list.
    """
    winners = []
    for i in range(0, len(bracket_half), 2):
        if i + 1 >= len(bracket_half):
            winners.append(bracket_half[i])
            continue
        team_a, team_b = bracket_half[i], bracket_half[i + 1]
        prob = matchup_win_prob(team_a, team_b, model)
        if noise_std > 0:
            prob = prob + random.gauss(0, noise_std)
            prob = max(0.01, min(0.99, prob))
        winner = team_a if random.random() < prob else team_b
        winners.append(winner)
    return winners


def simulate_bracket(teams: list, algorithm: str) -> dict:
    """
    Simulate the full bracket with the given algorithm.

    algorithm: "SeedModel" | "UpsetModel" | "MonteCarlo"

    Returns dict:
      {
        "algorithm": str,
        "rounds": {region: [[{team},...], ...], "Final Four": [...], "Championship": [...]},
        "final_four": [team, team, team, team],
        "champion": team,
      }
    """
    model = "upset" if algorithm == "UpsetModel" else "seed"
    noise = 0.08 if algorithm == "MonteCarlo" else 0.0

    regions = {}
    for t in teams:
        regions.setdefault(t["region"], []).append(t)

    region_winners = {}
    all_rounds = {}

    for region, rteams in regions.items():
        ordered = _order_region_teams(rteams)
        rounds_in_region = [ordered]
        current = ordered
        while len(current) > 1:
            current = _play_round(current, model, noise)
            rounds_in_region.append(current)
        region_winners[region] = current[0]
        all_rounds[region] = rounds_in_region

    # Final Four: South vs East, West vs Midwest (standard bracket)
    region_order = ["South", "East", "West", "Midwest"]
    available = {r: region_winners[r] for r in region_order if r in region_winners}
    region_list = list(available.keys())

    # Pair up semi-finals
    semifinal_pairs = []
    if len(region_list) >= 4:
        semifinal_pairs = [(region_list[0], region_list[1]), (region_list[2], region_list[3])]
    elif len(region_list) == 2:
        semifinal_pairs = [(region_list[0], region_list[1])]

    final_four_teams = [available[r] for r in region_list]
    all_rounds["Final Four"] = [final_four_teams]

    finalists = []
    for ra, rb in semifinal_pairs:
        ta, tb = available[ra], available[rb]
        prob = matchup_win_prob(ta, tb, model)
        if noise > 0:
            prob = max(0.01, min(0.99, prob + random.gauss(0, noise)))
        winner = ta if random.random() < prob else tb
        finalists.append(winner)

    all_rounds["Final Four"].append(finalists)

    # Championship
    all_rounds["Championship"] = [finalists]
    if len(finalists) == 2:
        ta, tb = finalists[0], finalists[1]
        prob = matchup_win_prob(ta, tb, model)
        if noise > 0:
            prob = max(0.01, min(0.99, prob + random.gauss(0, noise)))
        champion = ta if random.random() < prob else tb
    elif len(finalists) == 1:
        champion = finalists[0]
    else:
        champion = None

    if champion:
        all_rounds["Championship"].append([champion])

    return {
        "algorithm": algorithm,
        "rounds": all_rounds,
        "final_four": final_four_teams,
        "champion": champion,
    }


# ── Monte Carlo aggregation ───────────────────────────────────────────────────

def run_monte_carlo(teams: list, n_simulations: int = 10_000) -> dict:
    """
    Run n_simulations bracket simulations with MonteCarlo algorithm.
    Returns win-count dicts per team per round-depth (0=R64 thru 5=Champion).
    """
    # Counts: team_name -> list[int] indexed by rounds reached (0..5)
    reach_counts = {t["name"]: [0] * 6 for t in teams}

    regions = {}
    for t in teams:
        regions.setdefault(t["region"], []).append(t)

    region_order = ["South", "East", "West", "Midwest"]

    for _ in range(n_simulations):
        # Per-region simulation
        region_winners = {}
        for region, rteams in regions.items():
            ordered = _order_region_teams(rteams)
            current = ordered
            round_idx = 0
            for t in current:
                reach_counts[t["name"]][round_idx] += 1
            while len(current) > 1:
                current = _play_round(current, "seed", noise_std=0.08)
                round_idx += 1
                for t in current:
                    reach_counts[t["name"]][round_idx] += 1
            region_winners[region] = current[0]

        # Final Four
        available = {r: region_winners[r] for r in region_order if r in region_winners}
        region_list = list(available.keys())
        if len(region_list) < 2:
            continue

        semifinal_pairs = []
        if len(region_list) >= 4:
            semifinal_pairs = [(region_list[0], region_list[1]), (region_list[2], region_list[3])]
        else:
            semifinal_pairs = [(region_list[0], region_list[1])]

        finalists = []
        for ra, rb in semifinal_pairs:
            ta, tb = available[ra], available[rb]
            prob = matchup_win_prob(ta, tb, "seed")
            prob = max(0.01, min(0.99, prob + random.gauss(0, 0.08)))
            winner = ta if random.random() < prob else tb
            finalists.append(winner)
            reach_counts[winner["name"]][4] += 1

        if len(finalists) == 2:
            ta, tb = finalists[0], finalists[1]
            prob = matchup_win_prob(ta, tb, "seed")
            prob = max(0.01, min(0.99, prob + random.gauss(0, 0.08)))
            champion = ta if random.random() < prob else tb
            reach_counts[champion["name"]][5] += 1

    # Normalize to percentages
    win_pcts = {}
    for team_name, counts in reach_counts.items():
        win_pcts[team_name] = [round(c / n_simulations * 100, 2) for c in counts]

    return win_pcts


# ── Main prediction runner ────────────────────────────────────────────────────

def run_predictions() -> dict:
    """
    Run all three prediction algorithms and return a comprehensive results dict.
    Also saves bracket_results.json to disk.
    """
    print("Loading bracket data...")
    teams = load_bracket()
    print(f"  {len(teams)} teams loaded across {len(set(t['region'] for t in teams))} regions.")

    results = {
        "generated_at": datetime.now().isoformat(),
        "teams": teams,
        "rounds": {},
        "champion": {},
        "final_four": {},
        "consensus": [],
        "best_upset_picks": [],
        "monte_carlo_win_pcts": {},
    }

    # Run deterministic models
    for algo in ("SeedModel", "UpsetModel"):
        print(f"  Running {algo}...")
        sim = simulate_bracket(teams, algo)
        results["rounds"][algo] = _serialize_rounds(sim["rounds"])
        results["champion"][algo] = sim["champion"]
        results["final_four"][algo] = sim["final_four"]

    # Monte Carlo: single representative simulation + win-pct aggregation
    print("  Running MonteCarlo (10,000 simulations)...")
    mc_sim = simulate_bracket(teams, "MonteCarlo")
    results["rounds"]["MonteCarlo"] = _serialize_rounds(mc_sim["rounds"])
    results["champion"]["MonteCarlo"] = mc_sim["champion"]
    results["final_four"]["MonteCarlo"] = mc_sim["final_four"]

    win_pcts = run_monte_carlo(teams, n_simulations=10_000)
    results["monte_carlo_win_pcts"] = win_pcts

    # Consensus: teams all 3 algorithms agree reach Final Four
    ff_sets = [
        {t["name"] for t in results["final_four"]["SeedModel"]},
        {t["name"] for t in results["final_four"]["UpsetModel"]},
        {t["name"] for t in results["final_four"]["MonteCarlo"]},
    ]
    consensus_ff = ff_sets[0] & ff_sets[1] & ff_sets[2]
    results["consensus"] = sorted(consensus_ff)

    # Best upset picks: seeds 10-15 with >30% championship win chance in MonteCarlo
    upset_picks = []
    for team in teams:
        if 10 <= team["seed"] <= 15:
            champ_pct = win_pcts.get(team["name"], [0] * 6)[5]
            if champ_pct > 30.0:
                upset_picks.append({
                    "name": team["name"],
                    "seed": team["seed"],
                    "region": team["region"],
                    "champ_pct": champ_pct,
                })
    # Sort by championship % descending
    upset_picks.sort(key=lambda x: x["champ_pct"], reverse=True)
    results["best_upset_picks"] = upset_picks

    # Save to disk
    with open(RESULTS_PATH, "w") as f:
        json.dump(results, f, indent=2, default=str)
    print(f"  Results saved to {RESULTS_PATH}")

    return results


def _serialize_rounds(rounds: dict) -> dict:
    """Convert round data to a JSON-serializable dict."""
    out = {}
    for round_name, round_data in rounds.items():
        out[round_name] = []
        for stage in round_data:
            out[round_name].append([
                {"name": t["name"], "seed": t["seed"], "region": t["region"]}
                for t in stage
            ])
    return out


# ── Terminal display ──────────────────────────────────────────────────────────

def _team_label(team: dict) -> str:
    return f"({team['seed']}) {team['name']}"


def print_bracket(results: dict):
    """Print a formatted bracket summary to the terminal."""
    width = 72
    sep = "─" * width

    print()
    print("=" * width)
    print("  NCAA MARCH MADNESS 2026 — BRACKET PREDICTIONS".center(width))
    print(f"  Generated: {results.get('generated_at', 'N/A')}".center(width))
    print("=" * width)

    for algo in ("SeedModel", "UpsetModel", "MonteCarlo"):
        print()
        print(f"  ▶  {algo}".upper())
        print(sep)

        rounds = results["rounds"].get(algo, {})
        region_names = [r for r in rounds if r not in ("Final Four", "Championship")]

        for region in region_names:
            stages = rounds[region]
            print(f"\n  Region: {region}")
            round_labels = ["R64", "R32", "S16", "E8", "Champion"]
            for i, stage in enumerate(stages):
                label = round_labels[i] if i < len(round_labels) else f"Rd{i+1}"
                teams_str = "  vs  ".join(_team_label(t) for t in stage)
                print(f"    {label:5s}: {teams_str}")

        # Final Four
        ff_stages = rounds.get("Final Four", [])
        if ff_stages:
            print(f"\n  Final Four Participants:")
            for t in ff_stages[0]:
                print(f"    {_team_label(t)}")
        if len(ff_stages) > 1:
            print(f"\n  Championship Game:")
            for t in ff_stages[1]:
                print(f"    {_team_label(t)}")

        champ = results["champion"].get(algo)
        if champ:
            print(f"\n  {algo} CHAMPION: {_team_label(champ).upper()}")
        print(sep)

    # Consensus
    print()
    print("  CONSENSUS FINAL FOUR (all 3 models agree):")
    if results.get("consensus"):
        for name in results["consensus"]:
            print(f"    • {name}")
    else:
        print("    (No full consensus — models diverge)")

    # Upset picks
    print()
    print("  BEST UPSET PICKS (MonteCarlo >30% champ chance, seeds 10-15):")
    if results.get("best_upset_picks"):
        for pick in results["best_upset_picks"]:
            print(f"    • ({pick['seed']}) {pick['name']} [{pick['region']}] — {pick['champ_pct']}%")
    else:
        print("    (No extreme upsets predicted this year)")

    # Monte Carlo champion odds (top 8)
    print()
    print("  MONTE CARLO CHAMPIONSHIP ODDS (top 8 teams):")
    win_pcts = results.get("monte_carlo_win_pcts", {})
    champ_odds = [
        (name, pcts[5])
        for name, pcts in win_pcts.items()
        if len(pcts) > 5
    ]
    champ_odds.sort(key=lambda x: x[1], reverse=True)
    for name, pct in champ_odds[:8]:
        bar = "█" * int(pct / 2)
        print(f"    {name:<40s} {pct:5.1f}%  {bar}")

    print()
    print("=" * width)
    print()


# ── Entry point ───────────────────────────────────────────────────────────────

if __name__ == "__main__":
    random.seed(42)  # Reproducible output when run directly
    results = run_predictions()
    print_bracket(results)
