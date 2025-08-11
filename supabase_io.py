# supabase_io.py
from dotenv import load_dotenv, find_dotenv
load_dotenv(find_dotenv(), override=False)

import os
from typing import List, Dict
from datetime import datetime, timezone, date
from supabase import create_client, Client

NUMERIC_FIELDS = ["impressions", "clicks", "likes", "comments", "shares", "reach"]

def _safe_int(v):
    try:
        return int(v or 0)
    except:
        try:
            return int(float(str(v).replace(',', '')))
        except:
            return 0

def _recompute_rates(rec: Dict):
    impressions = _safe_int(rec.get("impressions"))
    clicks = _safe_int(rec.get("clicks"))
    likes = _safe_int(rec.get("likes"))
    comments = _safe_int(rec.get("comments"))
    shares = _safe_int(rec.get("shares"))
    ctr = round((clicks / impressions) * 100.0, 6) if impressions > 0 else 0.0
    er = round(((likes + comments + shares + clicks) / impressions) * 100.0, 6) if impressions > 0 else 0.0
    rec["ctr"] = ctr
    rec["engagement_rate"] = min(er, 100.0)
    return rec

class SupabaseIO:
    def __init__(self):
        url = os.getenv("SUPABASE_URL")
        key = os.getenv("SUPABASE_SERVICE_ROLE_KEY")
        if not url or not key:
            raise ValueError("SUPABASE_URL and SUPABASE_SERVICE_ROLE_KEY are required")
        self.client: Client = create_client(url, key)
        self.company_id = os.getenv("COMPANY_ID", "wentors")
        self.company_name = os.getenv("COMPANY_NAME", "Wentors")
        # Update policy: 'max' keeps higher of existing vs incoming. 'new' overwrites with incoming.
        self.update_policy = (os.getenv("POST_UPDATE_POLICY") or "max").lower().strip()

    def upsert_posts(self, posts: List[Dict]):
        if not posts:
            return

        # Ensure company_id on all
        posts = [dict(p, company_id=self.company_id) for p in posts]

        # 1) Find existing ids
        ids = [p.get("post_id") for p in posts if p.get("post_id")]
        existing_ids = set()
        existing_rows = {}
        for i in range(0, len(ids), 1000):
            batch = ids[i:i+1000]
            res = self.client.table("post_analytics") \
                .select("post_id, impressions, clicks, likes, comments, shares, reach") \
                .in_("post_id", batch) \
                .execute()
            if res.data:
                for r in res.data:
                    existing_ids.add(r["post_id"])
                    existing_rows[r["post_id"]] = r

        # 2) Split into inserts vs updates; for updates, optionally merge metrics
        new_posts, update_posts = [], []
        for p in posts:
            q = p.copy()
            q.pop("date_collected", None)  # never touch this on update; DB sets on insert
            if p["post_id"] in existing_ids:
                if self.update_policy == "max":
                    base = existing_rows.get(p["post_id"], {})
                    for f in NUMERIC_FIELDS:
                        q[f] = max(_safe_int(base.get(f)), _safe_int(q.get(f)))
                    _recompute_rates(q)
                # else 'new' — take incoming values as-is (rates already computed by caller)
                update_posts.append(q)
            else:
                # New row: let DB set date_collected and created_at
                new_posts.append(q)

        # 3) Insert new
        for i in range(0, len(new_posts), 100):
            self.client.table("post_analytics").insert(new_posts[i:i+100]).execute()

        # 4) Update existing
        for i in range(0, len(update_posts), 100):
            self.client.table("post_analytics").upsert(
                update_posts[i:i+100],
                on_conflict="post_id"
            ).execute()

    def insert_post_metrics_history(self, posts: List[Dict]):
        """Snapshot today's metrics per post into post_metrics_history."""
        if not posts:
            return
        today = date.today()
        rows = []
        now_iso = datetime.now(timezone.utc).isoformat()
        for p in posts:
            rows.append({
                "company_id": self.company_id,
                "post_id": p.get("post_id"),
                "observed_at": now_iso,
                "observed_date": today.isoformat(),
                "post_date": p.get("post_date"),
                "impressions": _safe_int(p.get("impressions")),
                "clicks": _safe_int(p.get("clicks")),
                "likes": _safe_int(p.get("likes")),
                "comments": _safe_int(p.get("comments")),
                "shares": _safe_int(p.get("shares")),
                "reach": _safe_int(p.get("reach")),
                "ctr": float(p.get("ctr") or 0),
                "engagement_rate": float(p.get("engagement_rate") or 0),
            })
        for i in range(0, len(rows), 200):
            self.client.table("post_metrics_history").upsert(
                rows[i:i+200],
                on_conflict="company_id,post_id,observed_date"
            ).execute()

    def upsert_followers(self, records: List[Dict]):
        if not records:
            return
        for i in range(0, len(records), 200):
            self.client.table("follower_analytics").upsert(
                records[i:i+200],
                on_conflict='company_id,demographic_type,demographic_value,date_collected'
            ).execute()

    def upsert_company_summary(self, summary: Dict):
        summary = dict(summary)
        summary["company_id"] = self.company_id
        summary["company_name"] = self.company_name
        summary.setdefault("date_collected", datetime.utcnow().isoformat())
        self.client.table("company_analytics").upsert(summary, on_conflict='company_id').execute()

    def insert_analytics_history(self, snapshot: Dict):
        snap = dict(snapshot)
        snap["company_id"] = self.company_id
        snap["company_name"] = self.company_name
        snap.setdefault("date_collected", datetime.utcnow().isoformat())
        self.client.table("analytics_history").insert(snap).execute()

    def get_current_followers(self) -> int:
        try:
            recent = self.client.table("follower_analytics") \
                .select("total_followers") \
                .eq("company_id", self.company_id) \
                .order("date_collected", desc=True).limit(1).execute()
            if recent.data and recent.data[0].get("total_followers"):
                return int(recent.data[0]["total_followers"])
        except:
            pass
        try:
            comp = self.client.table("company_analytics") \
                .select("followers_count") \
                .eq("company_id", self.company_id) \
                .order("date_collected", desc=True).limit(1).execute()
            if comp.data and comp.data[0].get("followers_count"):
                return int(comp.data[0]["followers_count"])
        except:
            pass
        return 0