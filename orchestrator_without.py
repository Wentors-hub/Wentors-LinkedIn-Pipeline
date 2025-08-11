
import os
import time
import logging
from dotenv import load_dotenv

from export_ingester import LinkedInExportIngestor

# Import conditionally (IMAP optional)
FETCH_MODE = os.getenv("FETCH_MODE", "local").lower()  # 'imap' or 'local'

EmailExportFetcher = None
if FETCH_MODE == "imap":
    try:
        from email_export_fetcher import EmailExportFetcher
    except Exception:
        EmailExportFetcher = None

def main_loop():
    load_dotenv()
    logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(name)s: %(message)s")
    logger = logging.getLogger("orchestrator")

    os.makedirs(os.getenv("LINKEDIN_DATA_PATH", "./linkedin_exports/"), exist_ok=True)
    os.makedirs(os.getenv("VALIDATION_REPORTS_PATH", "./validation_reports/"), exist_ok=True)

    interval_min = int(os.getenv("ORCHESTRATOR_INTERVAL_MIN", "30"))
    ingestor = LinkedInExportIngestor()

    fetcher = None
    if FETCH_MODE == "imap" and EmailExportFetcher:
        try:
            fetcher = EmailExportFetcher()
            logger.info("Email fetcher (IMAP) enabled.")
        except Exception as e:
            logger.warning(f"IMAP fetcher not initialized: {e}. Falling back to local mode.")

    if FETCH_MODE != "imap":
        logger.info("Running in LOCAL mode (no IMAP). Drop files into ./linkedin_exports and they will be ingested.")

    logger.info("Starting orchestrator")
    while True:
        try:
            # If IMAP enabled, try to fetch email attachments
            if fetcher:
                new_files = fetcher.fetch_new_exports()
                if new_files:
                    logger.info(f"Downloaded {len(new_files)} new export file(s).")

            # Always scan local folder
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


