# orchestrator.py
import os
import time
import logging
from dotenv import load_dotenv

from export_ingester import LinkedInExportIngestor
from file_collector import collect_downloads

# Modes
FETCH_MODE = os.getenv("FETCH_MODE", "local").lower()

EmailExportFetcher = None
if FETCH_MODE == "imap":
    try:
        from email_export_fetcher import EmailExportFetcher
    except Exception:
        EmailExportFetcher = None

try:
    from digest_ingester import DigestIngestor
except Exception:
    DigestIngestor = None

def main_loop():
    load_dotenv()
    logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(name)s: %(message)s")
    logger = logging.getLogger("orchestrator")

    linkedin_path = os.getenv("LINKEDIN_DATA_PATH", "./linkedin_exports")
    reports_path = os.getenv("VALIDATION_REPORTS_PATH", "./validation_reports")
    os.makedirs(linkedin_path, exist_ok=True)
    os.makedirs(reports_path, exist_ok=True)

    interval_min = int(os.getenv("ORCHESTRATOR_INTERVAL_MIN", "5"))
    downloads_src = os.getenv("DOWNLOAD_SOURCE_DIR")

    ingestor = LinkedInExportIngestor()
    digest_ingestor = DigestIngestor() if DigestIngestor else None

    fetcher = None
    if FETCH_MODE == "imap" and EmailExportFetcher:
        try:
            fetcher = EmailExportFetcher()
            logger.info("IMAP fetcher enabled.")
        except Exception as e:
            logger.warning(f"IMAP fetcher not initialized: {e}. Running in local mode.")

    if FETCH_MODE != "imap":
        logger.info("Running in LOCAL mode (no IMAP).")
    if downloads_src:
        logger.info(f"Download collector enabled. Watching: {downloads_src}")

    logger.info("Starting orchestrator loop")
    while True:
        try:
            # 1) Move finished downloads into linkedin_exports (if configured)
            if downloads_src:
                moved = collect_downloads(downloads_src, linkedin_path)
                if moved:
                    logger.info(f"Moved {len(moved)} downloaded file(s) into {linkedin_path}")

            # 2) IMAP: fetch attachments + digest emails
            if fetcher:
                new_files = fetcher.fetch_new_exports()
                if new_files:
                    logger.info(f"IMAP fetched {len(new_files)} export file(s).")

                digests = fetcher.fetch_digests()
                if digests and digest_ingestor:
                    added = digest_ingestor.ingest_digests(digests)
                    logger.info(f"Ingested {added} digest summary record(s).")

            # 3) Ingest what's in linkedin_exports
            posts_count, demo_count = ingestor.scan_and_process_folder()
            if posts_count or demo_count:
                logger.info(f"Ingest complete. Posts: {posts_count}, Demographics: {demo_count}")
            else:
                logger.info("No new files to ingest.")

        except Exception as e:
            logger.exception(f"Orchestrator error: {e}")

        time.sleep(interval_min * 60)

if __name__ == "__main__":
    main_loop()