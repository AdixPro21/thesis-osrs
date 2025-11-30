import csv
import os
import random
import time

import requests
from bs4 import BeautifulSoup

# -------------------------------------------------------------------
# Paths and constants
# -------------------------------------------------------------------

# Base directory = one level above this script file (e.g. project root)
BASE_DIR = os.path.abspath(os.path.join(os.path.dirname(__file__), ".."))

# Data directory at project root
DATA_DIR = os.path.join(BASE_DIR, "data")

# CSV output inside the project-level data folder
OUTPUT_CSV = os.path.join(DATA_DIR, "players_list.csv")

BASE_URL = "https://secure.runescape.com/m=hiscore_oldschool/overall?table={SKILL_TABLES}#headerHiscores"

# Mapping from skill name to hiscore table index
SKILL_TABLES = {
    "overall": 0,
    "attack": 1,
    "defence": 2,
    "strength": 3,
    "hitpoints": 4,
    "ranged": 5,
    "prayer": 6,
    "magic": 7,
    "cooking": 8,
    "woodcutting": 9,
    "fletching": 10,
    "fishing": 11,
    "firemaking": 12,
    "crafting": 13,
    "smithing": 14,
    "mining": 15,
    "herblore": 16,
    "agility": 17,
    "thieving": 18,
    "slayer": 19,
    "farming": 20,
    "runecraft": 21,
    "hunter": 22,
    "construction": 23,
}

MAX_PAGE = 20000                 # random page between 1 and 20000
TARGET_PER_SKILL = 2             # how many nicknames per skill

HEADERS = {
    "User-Agent": "ThesisOSRS-Sampler/1.0 (contact@example.com)"
}


def load_existing_names(csv_path: str) -> set[str]:
    """
    Load existing player names from the CSV to avoid duplicates.
    Expects a column named 'player_name' if the file exists.
    """
    if not os.path.exists(csv_path):
        return set()

    names = set()
    with open(csv_path, newline="", encoding="utf-8") as f:
        reader = csv.DictReader(f)
        if "player_name" not in reader.fieldnames:
            raise ValueError(
                f"{csv_path} exists but has no 'player_name' column."
            )
        for row in reader:
            name = row["player_name"].strip()
            if name:
                names.add(name)
    return names


def fetch_page_names(table_index: int, page: int) -> list[str]:
    """
    Fetch all player names from a given hiscore page for a given skill table.
    Returns a list of RSNs as strings.
    """
    params = {"table": table_index, "page": page}
    resp = requests.get(BASE_URL, params=params, headers=HEADERS, timeout=10)
    resp.raise_for_status()

    soup = BeautifulSoup(resp.text, "html.parser")

    # Find the main hiscore table (the one with player rows)
    table = soup.find("table")
    if table is None:
        return []

    rows = table.find_all("tr")
    names: list[str] = []

    # Skip header row (first <tr>)
    for row in rows[1:]:
        # The player name is inside an <a> link in the row
        link = row.find("a")
        if not link:
            continue

        name = link.get_text(strip=True)
        if name:
            names.append(name)

    return names


def sample_names_for_skill(
    skill: str,
    table_index: int,
    existing_names: set[str],
    target: int
) -> list[str]:
    """
    For a given skill (table), sample up to `target` new names that are not in existing_names.
    Uses random pages in [1, MAX_PAGE].
    """
    collected: list[str] = []
    attempts = 0
    max_attempts = 100  # safety to avoid infinite loops

    while len(collected) < target and attempts < max_attempts:
        attempts += 1
        page = random.randint(1, MAX_PAGE)
        print(f"[{skill}] Trying page {page}...")

        try:
            names_on_page = fetch_page_names(table_index, page)
        except Exception as e:
            print(f"  Error fetching page {page} for {skill}: {e}")
            time.sleep(1)
            continue

        if not names_on_page:
            time.sleep(0.5)
            continue

        random.shuffle(names_on_page)

        for name in names_on_page:
            if name not in existing_names and name not in collected:
                collected.append(name)
                print(f"  -> added {name}")
                if len(collected) >= target:
                    break

        # Be polite to the server
        time.sleep(0.5)

    if len(collected) < target:
        print(
            f"[{skill}] Warning: only collected {len(collected)} names "
            f"after {attempts} attempts."
        )

    return collected


def append_names_to_csv(csv_path: str, new_rows: list[dict], existing: bool):
    """
    Append new_rows to CSV. If file doesn't exist yet, write header.
    new_rows: list of dicts with keys 'player_name', 'source_skill'.
    """
    fieldnames = ["player_name", "source_skill"]

    mode = "a" if existing else "w"
    with open(csv_path, mode, newline="", encoding="utf-8") as f:
        writer = csv.DictWriter(f, fieldnames=fieldnames)
        if not existing:
            writer.writeheader()
        for row in new_rows:
            writer.writerow(row)


if __name__ == "__main__":
    # Ensure the project-level /data directory exists
    os.makedirs(DATA_DIR, exist_ok=True)

    # 1. Load existing names from CSV (if it exists)
    existing_names = load_existing_names(OUTPUT_CSV)
    print(f"Loaded {len(existing_names)} existing names from {OUTPUT_CSV}")

    all_new_rows: list[dict] = []

    # 2. For each skill, sample TARGET_PER_SKILL new names
    for skill, table_index in SKILL_TABLES.items():
        print(f"\n=== Sampling for skill: {skill} (table {table_index}) ===")
        new_names = sample_names_for_skill(
            skill,
            table_index,
            existing_names,
            TARGET_PER_SKILL,
        )

        # Update the in-memory set so future skills don't reuse these names
        for name in new_names:
            existing_names.add(name)
            all_new_rows.append(
                {"player_name": name, "source_skill": skill}
            )

    # 3. Append everything to CSV (creating it if needed)
    file_already_exists = os.path.exists(OUTPUT_CSV)
    append_names_to_csv(OUTPUT_CSV, all_new_rows, existing=file_already_exists)

    print(
        f"\nDone. Added {len(all_new_rows)} new names. "
        f"Total unique names now: {len(existing_names)}"
    )
