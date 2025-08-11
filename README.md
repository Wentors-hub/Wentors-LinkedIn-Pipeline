# Wentors LinkedIn Company Analytics Pipeline

Automated pipeline to ingest LinkedIn Company Page analytics (posts + follower demographics) into Supabase — with no scraping and no API.

Workflow
- Click Export in LinkedIn Analytics (Content/Updates or Followers).
- Browser downloads the file to your Downloads folder.
- A watcher moves it to ./linkedin_exports and triggers ingestion.
- Data is upserted into Supabase (deduped; metrics merged).

Prerequisites
- Python 3.10+
- Supabase project (URL + Service Role Key)
- Windows (watcher uses watchdog; works on macOS/Linux too)
- No LinkedIn API required.

Setup
1. Clone the repo and install deps:
   python -m venv venv
   venv\Scripts\activate (Windows) | source venv/bin/activate (macOS/Linux)
   pip install -r requirements.txt

2. Create .env from .env.example:
   - SUPABASE_URL=...
   - SUPABASE_SERVICE_ROLE_KEY=...
   - COMPANY_ID=wentors
   - COMPANY_NAME=Wentors
   - LINKEDIN_DATA_PATH=./linkedin_exports
   - LINKEDIN_DATE_DMY=true   # if your exports are DD/MM/YYYY
   - DOWNLOAD_SOURCE_DIR=C:\Users\<you>\Downloads

3. Create runtime folders:
   mkdir linkedin_exports validation_reports

Database migration (run once in Supabase SQL editor)
- Enforce follower dedupe per day and maintain updated_at on post updates:
  create unique index if not exists follower_daily_unique_idx
  on follower_analytics (company_id, demographic_type, demographic_value, date_collected);

  create or replace function set_updated_at() returns trigger as $$
  begin new.updated_at = now(); return new; end; $$ language plpgsql;

  drop trigger if exists trg_set_updated_at on post_analytics;
  create trigger trg_set_updated_at
  before update on post_analytics
  for each row execute procedure set_updated_at();

Optional: per-post daily history table:
  create table if not exists post_metrics_history (
    id bigserial primary key,
    company_id text not null,
    post_id text not null,
    observed_at timestamptz default now(),
    observed_date date not null default (current_date),
    post_date timestamptz,
    impressions integer default 0,
    clicks integer default 0,
    likes integer default 0,
    comments integer default 0,
    shares integer default 0,
    reach integer default 0,
    ctr numeric default 0,
    engagement_rate numeric default 0
  );
  create unique index if not exists post_metrics_hist_daily_uniq
  on post_metrics_history (company_id, post_id, observed_date);

Run modes
- Instant (recommended): download_watcher.py (watches Downloads; moves + ingests)
  python download_watcher.py

- Polling: orchestrator.py (every N minutes)
  FETCH_MODE=local
  DOWNLOAD_SOURCE_DIR=... in .env
  ORCHESTRATOR_INTERVAL_MIN=5
  python orchestrator.py

Verify
- A new export should show logs: “Inserted/updated N posts …”
- Supabase checks:
  select count(*) from post_analytics where company_id='wentors';
  select distinct post_type from post_analytics where company_id='wentors';
  select * from follower_analytics where company_id='wentors' order by date_collected desc limit 20;

Notes
- No scraping, no API: compliant with LinkedIn ToS.
- Deduped: posts by post_id; demographics by (company_id, type, value, day).
- Metrics merge: by default, keep higher values on re-ingest (POST_UPDATE_POLICY=max).