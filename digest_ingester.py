# digest_ingester.py
from datetime import datetime
from supabase_io import SupabaseIO
from email_digest_parser import parse_page_digest

class DigestIngestor:
    def __init__(self):
        self.sb = SupabaseIO()

    def ingest_digests(self, digests: list[dict]) -> int:
        count = 0
        for d in digests:
            body = d.get("body") or ""
            summary = parse_page_digest(body)
            if not summary:
                continue
            self.sb.upsert_company_summary(summary)
            try:
                snap = dict(summary)
                snap["date_collected"] = datetime.utcnow().isoformat()
                self.sb.insert_analytics_history(snap)
            except:
                pass
            count += 1
        return count