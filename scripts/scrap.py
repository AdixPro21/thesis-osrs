import requests
from bs4 import BeautifulSoup
import pandas as pd
import time

# -----------------------------
# CONFIG
# -----------------------------
LEADERBOARD_URL = "https://secure.runescape.com/m=hiscore_oldschool/overall?category_type=1&table=83"  # from Safari
OUTPUT_FILE = "data/vorkath_leaderboard.csv"
SLEEP_TIME = 2  # seconds between pages
NUM_PAGES = 10     # start with just 1 page while debugging

HEADERS = {
    "User-Agent": (
        "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) "
        "AppleWebKit/537.36 (KHTML, like Gecko) "
        "Chrome/120.0.0.0 Safari/537.36"
    )
}

# -----------------------------
# MAIN
# -----------------------------
all_rows = []

for page in range(1, NUM_PAGES + 1):
    # if your URL already has ?page= in it, remove this &page={page} part
    url = f"{LEADERBOARD_URL}&page={page}"
    print(f"Fetching page {page}: {url}")

    try:
        resp = requests.get(url, headers=HEADERS, timeout=15)
        print("Status code:", resp.status_code)
        if resp.status_code != 200:
            print("HTML snippet:", resp.text[:500])
            continue

        # PRINT first 500 chars so we see something
        print("First 500 chars of HTML:")
        print(resp.text[:500])

        soup = BeautifulSoup(resp.text, "html.parser")

        # 1) How many tables?
        tables = soup.find_all("table")
        print(f"Found {len(tables)} <table> elements")

        if not tables:
            continue

        # TEMP: just inspect the first table’s first few rows to see structure
        table = tables[0]
        rows = table.find_all("tr")
        print(f"First table has {len(rows)} rows (including header)")

        # Print first 3 rows' text so we can see columns
        for i, row in enumerate(rows[:3]):
            cols = [td.get_text(strip=True) for td in row.find_all("td")]
            print(f"Row {i} cols:", cols)

        # Now actually parse rows, skipping header if first row is header
        for row in rows[1:]:
            cols = [td.get_text(strip=True) for td in row.find_all("td")]
            if len(cols) < 3:
                continue  # not enough columns to be a player row

            # Here we *guess* col positions: adjust after you see the debug output
            rank = cols[0]
            username = cols[1]
            kc_str = cols[2].replace(",", "")

            try:
                kc = int(kc_str)
            except ValueError:
                # maybe this is a header or something weird
                continue

            all_rows.append({
                "rank": int(rank),
                "username": username,
                "vorkath_kc": kc
            })

        print(f"Collected rows so far: {len(all_rows)}")
        time.sleep(SLEEP_TIME)

    except Exception as e:
        print(f"Error on page {page}:", e)
        continue

# Save to CSV
if all_rows:
    df = pd.DataFrame(all_rows)
    df.to_csv(OUTPUT_FILE, index=False)
    print(f"Saved {len(df)} rows to {OUTPUT_FILE}")
else:
    print("No rows collected – check the debug prints above.")
