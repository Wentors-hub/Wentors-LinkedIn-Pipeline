# debug_dates.py
import os, re, csv
from pathlib import Path
from datetime import datetime
import pandas as pd

from export_ingester import load_posts_df_robust, find_column, extract_urn_from_url, make_post_id
from normalizer_utils import normalize_post_type, extract_hashtags, extract_mentions, compute_ctr

def parse_try(s: str, dmy: bool):
    try:
        return pd.to_datetime(s, dayfirst=dmy)
    except:
        return None

def likely_ambiguous(s: str):
    # e.g., "08/06/2025" where both day and month <= 12
    if not isinstance(s, str): return False
    m = re.match(r'^\s*(\d{1,2})[/-](\d{1,2})[/-](\d{2,4})', s.strip())
    if not m: return False
    a, b = int(m.group(1)), int(m.group(2))
    return a <= 12 and b <= 12

def main():
    data_dir = Path(os.getenv("LINKEDIN_DATA_PATH", "./linkedin_exports/"))
    files = sorted([p for p in data_dir.iterdir() if p.suffix.lower() in (".xls",".xlsx",".csv",".zip")])
    if not files:
        print("No files found.")
        return
    f = files[0]
    print(f"Analyzing {f.name}")
    df = load_posts_df_robust(f)
    if df.empty:
        print("No rows.")
        return

    created_col = find_column(df, ['created date','date','published'])
    post_link_col = find_column(df, ['post link','url','link','permalink'])
    content_type_col = find_column(df, ['content type','content format','format','media','type'])
    title_col = find_column(df, ['post title','title'])
    camp_start_col = find_column(df, ['campaign start date'])

    out = []
    for _, row in df.iterrows():
        raw = row.get(created_col)
        if isinstance(raw, datetime):
            # Already a datetime (from xls)
            continue
        s = str(raw) if raw is not None else ""
        if not s.strip():
            continue

        if likely_ambiguous(s):
            ts_dmy = parse_try(s, True)
            ts_mdy = parse_try(s, False)
            camp = row.get(camp_start_col)
            ts_camp = parse_try(str(camp), True) or parse_try(str(camp), False)

            choice = "dmy"  # default
            if ts_dmy and ts_mdy:
                # Heuristic: if campaign start exists, choose the one closest to it
                if ts_camp is not None:
                    diff_dmy = abs((ts_dmy - ts_camp).total_seconds())
                    diff_mdy = abs((ts_mdy - ts_camp).total_seconds())
                    choice = "dmy" if diff_dmy <= diff_mdy else "mdy"
                else:
                    # If no campaign start, prefer the one in plausible range (2015..now+1y)
                    low = datetime(2015,1,1)
                    hi = datetime(datetime.utcnow().year+1,12,31)
                    ok_dmy = (low <= ts_dmy <= hi)
                    ok_mdy = (low <= ts_mdy <= hi)
                    if ok_dmy and not ok_mdy: choice = "dmy"
                    elif ok_mdy and not ok_dmy: choice = "mdy"

                out.append({
                    "post_url": str(row.get(post_link_col) or ""),
                    "raw_created": s,
                    "dmy": ts_dmy.isoformat() if ts_dmy is not None else "",
                    "mdy": ts_mdy.isoformat() if ts_mdy is not None else "",
                    "campaign_start": ts_camp.isoformat() if ts_camp is not None else "",
                    "chosen": choice
                })

    if not out:
        print("No ambiguous created dates found.")
        return

    ts = datetime.utcnow().strftime("%Y%m%d_%H%M%S")
    out_path = Path("./validation_reports") / f"date_discrepancies_{ts}.csv"
    out_path.parent.mkdir(parents=True, exist_ok=True)
    with open(out_path, "w", newline="", encoding="utf-8") as fcsv:
        w = csv.DictWriter(fcsv, fieldnames=list(out[0].keys()))
        w.writeheader()
        w.writerows(out)
    print(f"Wrote {len(out)} discrepancies to {out_path}")

if __name__ == "__main__":
    main()