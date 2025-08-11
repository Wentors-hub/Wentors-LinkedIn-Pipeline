# export_ingester.py

import os
import re
import xlrd
import hashlib
import logging
import zipfile
from datetime import datetime, timezone
from pathlib import Path
from typing import List, Dict, Optional, Tuple

import pandas as pd

from normalizer_utils import normalize_post_type, extract_hashtags, extract_mentions, compute_ctr
from supabase_io import SupabaseIO

# Logger
LOG = logging.getLogger("export_ingester")
if not LOG.handlers:
    h = logging.StreamHandler()
    h.setFormatter(logging.Formatter("%(asctime)s %(levelname)s %(name)s: %(message)s"))
    LOG.addHandler(h)
LOG.setLevel(logging.INFO)

# ----------------------------
# Helpers and Parsing Utilities
# ----------------------------

def safe_int(value) -> int:
    try:
        if value is None or (isinstance(value, float) and pd.isna(value)):
            return 0
        s = str(value).replace('%', '').replace(',', '').strip()
        m = re.search(r'[\d,]+(?:\.\d+)?', s)
        if m:
            s = m.group().replace(',', '')
        return int(float(s))
    except:
        return 0

def safe_float(value) -> float:
    try:
        if value is None or (isinstance(value, float) and pd.isna(value)):
            return 0.0
        s = str(value).replace('%', '').replace(',', '').strip()
        m = re.search(r'[\d,]+(?:\.\d+)?', s)
        if m:
            s = m.group().replace(',', '')
        return float(s)
    except:
        return 0.0

def parse_date_smart(value) -> str:
    """Parse dates robustly; honor LINKEDIN_DATE_DMY env for day-first formats."""
    if value is None or (isinstance(value, float) and pd.isna(value)) or str(value).strip() == "":
        return datetime.utcnow().isoformat()
    if isinstance(value, datetime):
        return value.isoformat()

    prefer_dmy = os.getenv("LINKEDIN_DATE_DMY", "").lower() in ("1", "true", "yes", "y")
    s = str(value).strip()
    fmts_dmy = ['%d/%m/%Y', '%d/%m/%y', '%d-%m-%Y', '%d-%m-%y']
    fmts_mdy = ['%m/%d/%Y', '%m/%d/%y', '%m-%d-%Y', '%m-%d-%y']
    fmts_common = ['%Y-%m-%d', '%Y/%m/%d', '%B %d, %Y', '%b %d, %Y', '%d %B %Y', '%d %b %Y']
    fmts = (fmts_dmy + fmts_mdy + fmts_common) if prefer_dmy else (fmts_mdy + fmts_dmy + fmts_common)
    for f in fmts:
        try:
            return datetime.strptime(s, f).isoformat()
        except:
            continue
    try:
        return pd.to_datetime(value, dayfirst=prefer_dmy).isoformat()
    except:
        return datetime.utcnow().isoformat()

def extract_urn_from_url(url: str) -> str:
    if not url:
        return ""
    m = re.search(r'(urn:li:(?:activity|ugcPost):[0-9]+)', url)
    return m.group(1) if m else ""

def clean_rate(value) -> float:
    """
    Normalize a rate value that might be:
      - '0.36%'   -> 0.36
      - 0.0036    -> 0.36
      - 0.36      -> 0.36
    Always return percentage in 0–100.
    """
    if value is None or (isinstance(value, float) and pd.isna(value)):
        return 0.0
    s = str(value).strip()
    if s.endswith('%'):
        try:
            return float(s.replace('%', '').replace(',', '').strip())
        except:
            return 0.0
    try:
        val = float(s.replace(',', ''))
        return round(val * 100.0, 6) if 0 < val < 1 else round(val, 6)
    except:
        return 0.0

def find_column(df: pd.DataFrame, candidates: List[str]) -> Optional[str]:
    """Find a column in df by trying a list of candidate names (case-insensitive, substring allowed)."""
    cols = [str(c).strip() for c in df.columns]
    lower = [c.lower() for c in cols]
    for name in candidates:
        nl = name.lower()
        if nl in lower:
            return cols[lower.index(nl)]
        for i, c in enumerate(lower):
            if nl in c:
                return cols[i]
    return None

def make_post_id(urn: str, url: str, content: str, date_iso: str) -> str:
    base = urn or url or f"{(content or '')[:100]}|{(date_iso or '')[:10]}"
    return hashlib.md5(base.encode()).hexdigest()[:16]

# ----------------------------
# XLS reading via xlrd (bypass pandas engine/version checks)
# ----------------------------

HEADER_HINTS = [
    'post', 'title', 'link', 'impressions', 'views', 'click', 'ctr',
    'likes', 'comments', 'repost', 'share', 'engagement', 'content type',
    'created', 'date', 'published'
]

def _guess_header_index(rows: list[list]) -> int:
    best_idx, best_score = 0, -1
    for i in range(min(10, len(rows))):
        row = rows[i]
        tokens = [str(x).strip().lower() for x in row if str(x).strip()]
        score = sum(any(h in t for h in HEADER_HINTS) for t in tokens)
        score += sum(1 for t in tokens if len(t) > 0)
        if score > best_score:
            best_idx, best_score = i, score
    return best_idx

def _sheet_to_dataframe_with_dates(sheet: xlrd.sheet.Sheet, datemode: int) -> pd.DataFrame:
    rows = []
    for r in range(sheet.nrows):
        row_vals = []
        for c in range(sheet.ncols):
            ct = sheet.cell_type(r, c)
            val = sheet.cell_value(r, c)
            if ct == xlrd.XL_CELL_DATE:
                try:
                    dt = xlrd.xldate.xldate_as_datetime(val, datemode)
                    row_vals.append(dt)
                except Exception:
                    row_vals.append(val)
            else:
                row_vals.append(val)
        rows.append(row_vals)

    if not rows:
        return pd.DataFrame()

    header_idx = _guess_header_index(rows)
    header = [str(x).strip() if str(x).strip() else f"Column_{i}" for i, x in enumerate(rows[header_idx])]
    data_rows = rows[header_idx + 1:]

    fixed_rows = []
    for r in data_rows:
        if all((str(x).strip() == '' for x in r)):
            continue
        if len(r) < len(header):
            r = list(r) + [''] * (len(header) - len(r))
        elif len(r) > len(header):
            r = r[:len(header)]
        fixed_rows.append(r)

    df = pd.DataFrame(fixed_rows, columns=header)
    df = df.dropna(how='all').dropna(axis=1, how='all').reset_index(drop=True)
    df.columns = [str(c).strip() for c in df.columns]
    return df

def read_xls_posts_via_xlrd(file_path: Path) -> pd.DataFrame:
    book = xlrd.open_workbook(file_path)
    names = book.sheet_names()
    target = next((n for n in names if 'post' in n.lower()), names[0])
    sheet = book.sheet_by_name(target)
    return _sheet_to_dataframe_with_dates(sheet, book.datemode)

def read_xls_all_sheets_via_xlrd(file_path: Path) -> List[Tuple[str, pd.DataFrame]]:
    book = xlrd.open_workbook(file_path)
    out = []
    for name in book.sheet_names():
        sheet = book.sheet_by_name(name)
        df = _sheet_to_dataframe_with_dates(sheet, book.datemode)
        out.append((name, df))
    return out

# ----------------------------
# Robust file loader (CSV/XLS/XLSX/ZIP)
# ----------------------------

def load_posts_df_robust(file_path: Path) -> pd.DataFrame:
    """Load posts data from CSV/XLS/XLSX/ZIP. For Excel, selects a sheet containing 'post'."""
    ext = file_path.suffix.lower()

    # ZIP
    if ext == ".zip":
        try:
            with zipfile.ZipFile(file_path, "r") as z:
                members = z.namelist()
                candidates = [m for m in members if m.lower().endswith(('.csv', '.xls', '.xlsx'))]
                if not candidates:
                    return pd.DataFrame()
                preferred = [c for c in candidates if "post" in c.lower()]
                target = preferred[0] if preferred else candidates[0]
                extract_dir = file_path.parent / ("unzipped_" + file_path.stem)
                extract_dir.mkdir(exist_ok=True)
                z.extract(target, path=extract_dir)
                inner_path = extract_dir / target
                return load_posts_df_robust(inner_path)
        except Exception as e:
            LOG.warning(f"Zip read failed for {file_path.name}: {e}")
            return pd.DataFrame()

    # XLSX
    if ext == ".xlsx":
        if zipfile.is_zipfile(file_path):
            try:
                xl = pd.ExcelFile(file_path, engine='openpyxl')
                post_sheets = [s for s in xl.sheet_names if 'post' in s.lower()]
                target_sheet = post_sheets[0] if post_sheets else xl.sheet_names[0]
                return pd.read_excel(file_path, sheet_name=target_sheet, engine='openpyxl', header=0)
            except Exception as e:
                LOG.warning(f"Openpyxl failed for {file_path.name}: {e}")
        # Fallback: misnamed CSV
        for enc in ['utf-8', 'cp1252', 'latin-1', 'iso-8859-1', 'utf-16']:
            try:
                return pd.read_csv(file_path, encoding=enc)
            except:
                continue
        return pd.DataFrame()

    # XLS via xlrd (manual)
    if ext == ".xls":
        try:
            return read_xls_posts_via_xlrd(file_path)
        except Exception as e:
            LOG.warning(f"xlrd direct read failed for {file_path.name}: {e}")
            return pd.DataFrame()

    # CSV
    if ext == ".csv":
        for enc in ['utf-8', 'cp1252', 'latin-1', 'iso-8859-1', 'utf-16']:
            try:
                return pd.read_csv(file_path, encoding=enc)
            except:
                continue
        return pd.DataFrame()

    # Unknown
    try:
        return pd.read_csv(file_path)
    except:
        return pd.DataFrame()

# ----------------------------
# Ingestor Class
# ----------------------------

class LinkedInExportIngestor:
    def __init__(self):
        self.sb = SupabaseIO()
        self.company_id = os.getenv("COMPANY_ID", "wentors")
        self.company_name = os.getenv("COMPANY_NAME", "Wentors")
        self.data_path = Path(os.getenv("LINKEDIN_DATA_PATH", "./linkedin_exports/"))
        self.reports_path = Path(os.getenv("VALIDATION_REPORTS_PATH", "./validation_reports/"))
        self.reports_path.mkdir(parents=True, exist_ok=True)

    def scan_and_process_folder(self) -> Tuple[int, int]:
        """Scan the folder for new files and process them. Returns (posts_count, demographics_count)."""
        if not self.data_path.exists():
            self.data_path.mkdir(parents=True, exist_ok=True)
            LOG.info(f"Created data folder: {self.data_path}")
            return 0, 0

        files = [p for p in self.data_path.iterdir()
                 if p.is_file() and p.suffix.lower() in ('.csv', '.xls', '.xlsx', '.zip') and p.name.lower() != ".ds_store"]

        if not files:
            LOG.info("No files found to process.")
            return 0, 0

        posts_count = 0
        demos_count = 0

        for f in sorted(files):
            name = f.name.lower()
            try:
                if any(k in name for k in ['post', 'content', 'update', 'overview']):
                    posts_count += self.process_posts_file(f)
                elif any(k in name for k in ['demographic', 'audience', 'follower']):
                    demos_count += self.process_demographics_file(f)
                else:
                    # Try posts as default
                    posts_count += self.process_posts_file(f)
                # Archive file
                self._archive_file(f)
            except Exception as e:
                LOG.exception(f"Error processing {f}: {e}")

        return posts_count, demos_count

    def process_posts_file(self, file_path: Path) -> int:
        """Process a LinkedIn posts/content export file (CSV/XLS/XLSX/ZIP)."""
        df = load_posts_df_robust(file_path)
        if df.empty:
            LOG.warning(f"No data in posts file {file_path}")
            return 0

        # Map columns (based on your “All posts” sample)
        post_title_col = find_column(df, ['post title', 'title'])
        post_link_col = find_column(df, ['post link', 'url', 'link', 'permalink'])
        # Distribution (Organic/Sponsored) – not stored as post_type
        distribution_col = find_column(df, ['post type'])
        content_type_col = find_column(df, ['content type', 'content format', 'format', 'media', 'type'])
        created_col = find_column(df, ['created date', 'date', 'published'])
        impressions_col = find_column(df, ['impressions'])
        views_col = find_column(df, ['views', 'offsite views'])
        clicks_col = find_column(df, ['clicks', 'link clicks', 'unique clicks'])
        ctr_col = find_column(df, ['click through rate (ctr)', 'ctr'])
        likes_col = find_column(df, ['likes', 'reactions'])
        comments_col = find_column(df, ['comments'])
        reposts_col = find_column(df, ['reposts', 'shares', 'share'])
        engagement_rate_col = find_column(df, ['engagement rate', 'engagement'])

        posts = []

        for _, row in df.iterrows():
            post_title = str(row.get(post_title_col) or "").strip()
            if not post_title:
                continue

            post_url = str(row.get(post_link_col) or "").strip()
            created_iso = parse_date_smart(row.get(created_col)) if created_col else datetime.utcnow().isoformat()

            impressions = safe_int(row.get(impressions_col)) if impressions_col else 0
            if impressions == 0 and views_col:
                impressions = safe_int(row.get(views_col))

            clicks = safe_int(row.get(clicks_col)) if clicks_col else 0
            ctr_val = clean_rate(row.get(ctr_col)) if ctr_col else compute_ctr(clicks, impressions)

            likes = safe_int(row.get(likes_col)) if likes_col else 0
            comments = safe_int(row.get(comments_col)) if comments_col else 0
            shares = safe_int(row.get(reposts_col)) if reposts_col else 0

            # Engagement rate: recompute as percent for consistency
            if impressions > 0:
                engagement_rate = round(((likes + comments + shares + clicks) / impressions) * 100.0, 6)
            else:
                engagement_rate = clean_rate(row.get(engagement_rate_col)) if engagement_rate_col else 0.0

            raw_content_type = str(row.get(content_type_col) or "")
            post_type = normalize_post_type(raw_content_type, post_title, post_url, post_title)

            urn = extract_urn_from_url(post_url)
            post_id = make_post_id(urn, post_url, post_title, created_iso)

            hashtags = extract_hashtags(post_title)
            mentions = extract_mentions(post_title)

            post_record = {
                "post_id": post_id,
                "company_id": self.company_id,
                "post_content": post_title[:2000],
                "post_date": created_iso,
                "impressions": impressions,
                "clicks": clicks,
                "likes": likes,
                "comments": comments,
                "shares": shares,
                "engagement_rate": min(engagement_rate, 100.0),
                "reach": impressions,
                "post_type": post_type,   # canonical: text/article/image/document/video
                "post_url": post_url,
                "post_title": post_title[:500],
                "ctr": ctr_val if ctr_val > 0 else compute_ctr(clicks, impressions),
                "hashtags": hashtags,
                "mentions": mentions
                # NOTE: no date_collected here; DB sets it on first insert
            }
            posts.append(post_record)

        if posts:
            self.sb.upsert_posts(posts)
            # Snapshot today's metrics per post (history table)
            try:
                self.sb.insert_post_metrics_history(posts)
            except Exception as e:
                LOG.debug(f"post_metrics_history snapshot failed: {e}")

            self._update_company_analytics(posts)
            LOG.info(f"Inserted/updated {len(posts)} posts from {file_path.name}")
            self._write_validation_report(posts, file_path.name)
            return len(posts)
        return 0

    def process_demographics_file(self, file_path: Path) -> int:
        """Process a LinkedIn demographics/followers export (multi-sheet Excel or CSV)."""
        total = 0
        ext = file_path.suffix.lower()

        if ext == '.xlsx':
            try:
                xl = pd.ExcelFile(file_path, engine='openpyxl')
                for sheet in xl.sheet_names:
                    df = pd.read_excel(file_path, sheet_name=sheet, engine='openpyxl', header=0)
                    total += self._process_demographics_df(df, sheet)
            except Exception as e:
                LOG.exception(f"Error reading Excel {file_path}: {e}")

        elif ext == '.xls':
            try:
                for sheet_name, df in read_xls_all_sheets_via_xlrd(file_path):
                    total += self._process_demographics_df(df, sheet_name)
            except Exception as e:
                LOG.exception(f"Error reading XLS {file_path}: {e}")

        else:
            df = load_posts_df_robust(file_path)
            total += self._process_demographics_df(df, "demographics")

        LOG.info(f"Inserted/updated {total} demographics records from {file_path.name}")
        return total

    def _process_demographics_df(self, df: pd.DataFrame, sheet_name: str) -> int:
        """Transform a single demographics sheet into follower_analytics rows."""
        if df is None or df.empty:
            return 0

        sheet_lower = (sheet_name or 'general').lower()
        if any(k in sheet_lower for k in ['location', 'country', 'city', 'region', 'geography']):
            demo_type = 'location'
        elif any(k in sheet_lower for k in ['function', 'job', 'role', 'occupation']):
            demo_type = 'job_function'
        elif any(k in sheet_lower for k in ['seniority', 'level', 'experience']):
            demo_type = 'seniority'
        elif any(k in sheet_lower for k in ['company', 'organization', 'employer', 'size']):
            demo_type = 'company_size'
        else:
            demo_type = sheet_lower.replace(' ', '_') if sheet_name else 'general'

        value_col = None
        count_col = None
        percentage_col = None
        for col in df.columns:
            cl = str(col).lower().strip()
            if not value_col and any(k in cl for k in ['name', 'value', 'location', 'function', 'title', 'category']):
                value_col = col
            if not count_col and any(k in cl for k in ['count', 'number', 'followers', 'audience', 'members', 'total']):
                count_col = col
            if not percentage_col and any(k in cl for k in ['percentage', 'percent', '%', 'share', 'pct']):
                percentage_col = col

        if not value_col and len(df.columns) > 0:
            value_col = df.columns[0]
        if not count_col and len(df.columns) > 1:
            count_col = df.columns[1]

        # Normalize to midnight UTC to dedupe per day
        day_key_iso = datetime.utcnow().replace(hour=0, minute=0, second=0, microsecond=0, tzinfo=timezone.utc).isoformat()

        recs = []
        for _, row in df.iterrows():
            val = str(row.get(value_col) or "").strip()
            if not val:
                continue
            cnt = safe_int(row.get(count_col))
            pct = safe_float(row.get(percentage_col)) if percentage_col else 0.0
            if cnt <= 0 and pct <= 0:
                continue
            recs.append({
                "company_id": self.company_id,
                "date_collected": day_key_iso,
                "total_followers": self.sb.get_current_followers(),
                "new_followers": 0,
                "demographic_type": demo_type,
                "demographic_value": val[:255],
                "count": cnt,
                "percentage": min(pct, 100.0)
            })

        if recs:
            self.sb.upsert_followers(recs)
        return len(recs)

    def _update_company_analytics(self, posts: List[Dict]):
        """Update rollups in company_analytics and optionally store a snapshot in analytics_history."""
        if not posts:
            return
        total_impr = sum(p.get("impressions", 0) for p in posts)
        total_clicks = sum(p.get("clicks", 0) for p in posts)
        avg_er = round(sum(p.get("engagement_rate", 0.0) for p in posts) / (len(posts) or 1), 4)
        snapshot = {
            "followers_count": self.sb.get_current_followers(),
            "impressions": total_impr,
            "unique_impressions": total_impr,
            "clicks": total_clicks,
            "engagement_rate": avg_er,
            "reach": total_impr,
            "total_posts": len(posts),
            "avg_post_engagement": avg_er,
            "date_collected": datetime.utcnow().isoformat()
        }
        self.sb.upsert_company_summary(snapshot)
        try:
            self.sb.insert_analytics_history(snapshot)
        except Exception as e:
            LOG.debug(f"Could not insert analytics_history snapshot: {e}")

    def _write_validation_report(self, posts: List[Dict], source_name: str):
        """Write a CSV with the records that were sent to Supabase (for auditing)."""
        try:
            df = pd.DataFrame(posts)
            ts = datetime.utcnow().strftime("%Y%m%d_%H%M%S")
            safe_name = source_name.replace('.', '_')
            out = self.reports_path / f"validation_{ts}_{safe_name}.csv"
            df.to_csv(out, index=False)
        except Exception as e:
            LOG.debug(f"Failed to write validation report: {e}")

    def _archive_file(self, path: Path):
        """Move processed file into ./linkedin_exports/processed with a timestamped name."""
        try:
            archive_dir = self.data_path / "processed"
            archive_dir.mkdir(parents=True, exist_ok=True)
            ts = datetime.utcnow().strftime("%Y%m%d_%H%M%S")
            new_name = archive_dir / f"{ts}_{path.name}"
            path.rename(new_name)
        except Exception as e:
            LOG.warning(f"Failed to archive {path.name}: {e}")