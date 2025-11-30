import requests
from urllib.parse import quote_plus
import csv
import pandas as pd
import os
from datetime import date

# --- Paths ----------------------------------------------------------

BASE_DIR = os.path.abspath(os.path.join(os.path.dirname(__file__), ".."))
DATA_DIR = os.path.join(BASE_DIR, "data")

HISCORES_URL = "https://secure.runescape.com/m=hiscore_oldschool/index_lite.ws?player={name}"

SKILLS = [
    "overall",
    "attack",
    "defence",
    "strength",
    "hitpoints",
    "ranged",
    "prayer",
    "magic",
    "cooking",
    "woodcutting",
    "fletching",
    "fishing",
    "firemaking",
    "crafting",
    "smithing",
    "mining",
    "herblore",
    "agility",
    "thieving",
    "slayer",
    "farming",
    "runecraft",
    "hunter",
    "construction",
]

# Boss list in official OSRS hiscore order
BOSSES = [
    "abyssal_sire",
    "alchemical_hydra",
    "amoxliatl",
    "araxxor",
    "artio",
    "barrows_chests",
    "bryophyta",
    "callisto",
    "calvarion",
    "cerberus",
    "chambers_of_xeric",
    "chambers_of_xeric_challenge_mode",
    "chaos_elemental",
    "chaos_fanatic",
    "commander_zilyana",
    "corporeal_beast",
    "crazy_archaeologist",
    "dagannoth_prime",
    "dagannoth_rex",
    "dagannoth_supreme",
    "deranged_archaeologist",
    "doom_of_mokhaiotl",
    "duke_sucellus",
    "general_graardor",
    "giant_mole",
    "grotesque_guardians",
    "hespori",
    "kalphite_queen",
    "king_black_dragon",
    "kraken",
    "kree_arra",
    "kril_tsutsaroth",
    "lunar_chests",
    "mimic",
    "nex",
    "nightmare",
    "phosanis_nightmare",
    "obor",
    "phantom_muspah",
    "sarachnis",
    "scorpia",
    "scurrius",
    "skotizo",
    "sol_heredit",
    "spindel",
    "tempoross",
    "gauntlet",
    "corrupted_gauntlet",
    "hueycoatl",
    "leviathan",
    "royal_titans",
    "whisperer",
    "theatre_of_blood",
    "theatre_of_blood_hard_mode",
    "thermonuclear_smoke_devil",
    "tombs_of_amascut",
    "tombs_of_amascut_expert_mode",
    "tzkal_zuk",
    "tztok_jad",
    "vardorvis",
    "venenatis",
    "vetion",
    "vorkath",
    "wintertodt",
    "yama",
    "zalcano",
    "zulrah",
]


def fetch_player_stats(player_name: str) -> dict:
    """Fetch OSRS hiscore stats + boss killcounts."""
    encoded = quote_plus(player_name)
    url = HISCORES_URL.format(name=encoded)

    resp = requests.get(url, timeout=10)
    if resp.status_code == 404:
        raise ValueError(f"Player '{player_name}' not on hiscores.")
    resp.raise_for_status()

    rows = list(csv.reader(resp.text.strip().split("\n")))
    stats = {}

    # --- Skills ---
    skill_rows = rows[: len(SKILLS)]
    for skill, row in zip(SKILLS, skill_rows):
        r, lvl, xp = row
        stats[skill] = {"rank": int(r), "level": int(lvl), "xp": int(xp)}

    # --- Activities (last N rows = bosses) ---
    activity_rows = rows[len(SKILLS) :]
    boss_count = len(BOSSES)
    boss_rows = activity_rows[-boss_count:] if len(activity_rows) >= boss_count else []

    boss_stats = {}
    for boss_name, row in zip(BOSSES, boss_rows):
        # Some rows have 2 values, some 3 → take first two
        if len(row) < 2:
            boss_stats[boss_name] = {"rank": None, "kc": None}
            continue

        rank_str, kc_str = row[0], row[1]

        try:
            rank = int(rank_str)
        except Exception:
            rank = None

        try:
            kc = int(kc_str)
        except Exception:
            kc = None

        # Convert -1 cases into clean values
        if rank is not None and rank < 0:
            rank = None
        if kc is not None and kc < 0:
            kc = 0

        boss_stats[boss_name] = {"rank": rank, "kc": kc}

    stats["bosses"] = boss_stats
    return stats


def load_player_names(csv_path: str) -> list:
    """Load player names from your list CSV."""
    if not os.path.exists(csv_path):
        raise FileNotFoundError(
            f"Player list not found at {csv_path}. "
            f"Make sure players_list.csv exists in the data/ folder."
        )

    df_raw = pd.read_csv(csv_path, header=None)

    # Detect headered vs headerless format
    if "player_name" in df_raw.iloc[0].values:
        df = pd.read_csv(csv_path)
        names = df["player_name"].astype(str).tolist()
    else:
        names = df_raw.iloc[:, 0].astype(str).tolist()

    return [n.strip() for n in names if isinstance(n, str) and n.strip()]


def build_database(player_names):
    """Build rows of (player_name, date, skills, bosses)."""
    today = date.today().isoformat()
    rows = []

    for name in player_names:
        print(f"Fetching: {name}…")
        try:
            stats = fetch_player_stats(name)
        except Exception as e:
            print(f"  Failed: {e}")
            continue

        row = {"player_name": name, "date": today}

        # Skills
        for skill in SKILLS:
            vals = stats[skill]
            row[f"{skill}_level"] = vals["level"]
            row[f"{skill}_xp"] = vals["xp"]
            row[f"{skill}_rank"] = vals["rank"]

        # Bosses
        for boss in BOSSES:
            vals = stats["bosses"].get(boss, {"kc": None, "rank": None})
            row[f"{boss}_kc"] = vals["kc"]
            row[f"{boss}_rank"] = vals["rank"]

        rows.append(row)

    return pd.DataFrame(rows)


if __name__ == "__main__":
    # 1. Load player list
    players_list_path = os.path.join(DATA_DIR, "players_list.csv")
    print(f"Using players list at: {players_list_path}")

    player_names = load_player_names(players_list_path)
    print(f"Loaded {len(player_names)} players.")

    # 2. Build today's rows
    df = build_database(player_names)

    # 3. Append to panel CSV
    os.makedirs(DATA_DIR, exist_ok=True)
    output_path = os.path.join(DATA_DIR, "players_stats.csv")

    file_exists = os.path.exists(output_path)

    df.to_csv(
        output_path,
        index=False,
        mode="a",
        header=not file_exists,
    )

    print(f"\n✔ Appended {len(df)} rows to {output_path}")
    print("✔ Panel database updated.")
