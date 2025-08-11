import os
from supabase import create_client
from normalizer_utils import normalize_post_type
from dotenv import load_dotenv

load_dotenv()

SUPABASE_URL = os.getenv("SUPABASE_URL")
SUPABASE_SERVICE_ROLE_KEY = os.getenv("SUPABASE_SERVICE_ROLE_KEY")
COMPANY_ID = os.getenv("COMPANY_ID", "wentors")

def run():
    client = create_client(SUPABASE_URL, SUPABASE_SERVICE_ROLE_KEY)
    rows = client.table("post_analytics").select("id, post_id, post_type, post_content, post_url, post_title").eq("company_id", COMPANY_ID).execute().data
    updates = []
    for r in rows:
        pt = (r.get("post_type") or "").lower()
        if pt in ("organic","sponsored","paid","boosted",""):
            new_pt = normalize_post_type(pt, r.get("post_content") or "", r.get("post_url") or "", r.get("post_title") or "")
            updates.append({"id": r["id"], "post_type": new_pt})
    for i in range(0, len(updates), 100):
        client.table("post_analytics").upsert(updates[i:i+100], on_conflict="id").execute()
    print(f"Reclassified {len(updates)} rows.")

if __name__ == "__main__":
    if not SUPABASE_URL or not SUPABASE_SERVICE_ROLE_KEY:
        print("Please set SUPABASE_URL and SUPABASE_SERVICE_ROLE_KEY in environment.")
    else:
        run()