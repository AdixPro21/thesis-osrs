import requests
import pandas as pd
import time
from datetime import datetime, timedelta
from urllib.parse import quote_plus

# =========================
# CONFIG
# =========================

# Input: CSV you already created from the Vorkath leaderboard
INPUT_CSV = "data/vorkath_leaderboard.csv"   # must have a 'username' column
OUTPUT_CSV = "data/vorkath_deltas.csv"

# Target date you want to compare against (e.g. one month ago)
# Option 1: hard-code a specific date:
TARGET_DATE_STR = "2023-10-01"   # <-- change this to the date you care about (YYYY-MM-DD)

# Option 2 (alternative): compute "about 30 days ago" automatically:
# TARGET_DATE_STR = (datetime.today() - timedelta(days=30)).strftime("%Y-%m-%d")

# How big a window around target date to look for snapshots (in days)
WINDOW_DAYS = 3

# Wise Old Man API config
WOM_BASE = "https://api.wiseoldman.net/v2"
HEADERS = {
    "User-Agent": "OSRS Thesis Herding (your_email@example.com)",
    "Content-Type": "application/json"
}

# Rate limiting: WOM allows 20 requests per 60 seconds.
# We use a conservative 3.2 seconds per request.
SLEEP_PER_CALL = 3.2


# =========================
# HELPER FUNCTIONS
# =========================

def get_current_vorkath_kc(username: str):
    """
    Get the current Vorkath KC and snapshot date from WOM /players/:username.
    Returns (kc, date_str) or (None, None) if not found.
    """
    url = f"{WOM_BASE}/players/{quote_plus(username)}"
    try:
        r = requests.get(url, headers=HEADERS, timeout=20)
    except Exception as e:
        print(f"[CURRENT] Network error for {username}: {e}")
        return None, None

    if r.status_code == 404:
        print(f"[CURRENT] {username} not found on WOM")
        return None, None
    if r.status_code != 200:
        print(f"[CURRENT] {username}: status {r.status_code} - {r.text[:200]}")
        return None, None

    data = r.json()

    # latestSnapshot contains bosses data
    latest = data.get("latestSnapshot")
    if not latest:
        print(f"[CURRENT] {username} has no snapshots")
        return None, None

    created_at = latest.get("createdAt")  # ISO timestamp
    bosses = latest.get("data", {}).get("bosses", {})
    vorkath = bosses.get("vorkath")  # metric name from WOM docs
    if not vorkath:
        print(f"[CURRENT] {username} has no Vorkath entry")
        return None, created_at

    kc = vorkath.get("kills")
    # Some trackers use -1 for "unranked"
    if kc is None or kc < 0:
        kc = None

    return kc, created_at


def get_vorkath_kc_on_date(username: str, target_date: datetime, window_days: int = 7):
    """
    Approximate the player's Vorkath KC around target_date using
    /players/:username/snapshots/timeline?metric=vorkath&startDate&endDate

    Returns (kc, date_str) or (None, None) if no snapshot in that window.
    """
    start = (target_date - timedelta(days=window_days)).isoformat() + "Z"
    end = (target_date + timedelta(days=window_days)).isoformat() + "Z"

    params = {
        "metric": "vorkath",   # from WOM Metric enum
        "startDate": start,
        "endDate": end
    }

    url = f"{WOM_BASE}/players/{quote_plus(username)}/snapshots/timeline"

    try:
        r = requests.get(url, headers=HEADERS, params=params, timeout=20)
    except Exception as e:
        print(f"[PAST] Network error for {username}: {e}")
        return None, None

    if r.status_code == 404:
        print(f"[PAST] {username} not found on WOM (timeline)")
        return None, None
    if r.status_code != 200:
        print(f"[PAST] {username}: status {r.status_code} - {r.text[:200]}")
        return None, None

    points = r.json()
    if not points:
        print(f"[PAST] No Vorkath timeline data in window for {username}")
        return None, None

    # points is a list of { value, rank, date }
    # choose the snapshot closest in time to target_date
    best = None
    best_delta = None
    for p in points:
        d_str = p.get("date")
        if not d_str:
            continue
        # WOM dates are ISO with Z
        d = datetime.fromisoformat(d_str.replace("Z", ""))
        delta = abs((d - target_date).total_seconds())
        if best is None or delta < best_delta:
            best = p
            best_delta = delta

    if not best:
        return None, None

    kc = best.get("value")
    if kc is None or kc < 0:
        kc = None

    return kc, best.get("date")


# =========================
# MAIN
# =========================

def main():
    target_date = datetime.fromisoformat(TARGET_DATE_STR)

    # Load usernames from your leaderboard CSV
    df_in = pd.read_csv(INPUT_CSV)

    # Try to guess username column
    if "username" in df_in.columns:
        usernames = df_in["username"].astype(str).tolist()
    elif "Name" in df_in.columns:
        usernames = df_in["Name"].astype(str).tolist()
    else:
        raise ValueError("Could not find a 'username' or 'Name' column in the input CSV.")

    print(f"Loaded {len(usernames)} usernames from {INPUT_CSV}")

    rows = []

    for i, username in enumerate(usernames, start=1):
        username = username.strip()
        if not username:
            continue

        print(f"\n=== {i}/{len(usernames)}: {username} ===")

        # 1) Get past KC around target date
        past_kc, past_date = get_vorkath_kc_on_date(username, target_date, WINDOW_DAYS)
        time.sleep(SLEEP_PER_CALL)

        # 2) Get current KC
        current_kc, current_date = get_current_vorkath_kc(username)
        time.sleep(SLEEP_PER_CALL)

        # 3) Compute delta (if both available)
        if past_kc is not None and current_kc is not None:
            delta = current_kc - past_kc
        else:
            delta = None

        rows.append({
            "username": username,
            "past_kc": past_kc,
            "past_date": past_date,
            "current_kc": current_kc,
            "current_date": current_date,
            "delta_kc": delta
        })

    df_out = pd.DataFrame(rows)
    df_out.to_csv(OUTPUT_CSV, index=False)
    print(f"\nDone. Saved {len(df_out)} rows to {OUTPUT_CSV}")


if __name__ == "__main__":
    main()
