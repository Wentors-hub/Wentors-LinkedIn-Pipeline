# email_export_fetcher.py
import os
import imaplib
import email
from email.header import decode_header
from datetime import datetime
from typing import List
from pathlib import Path

class EmailExportFetcher:
    def __init__(self):
        self.host = os.getenv("IMAP_HOST", "imap.gmail.com")
        self.port = int(os.getenv("IMAP_PORT", "993"))
        self.username = os.getenv("IMAP_USERNAME")
        self.password = os.getenv("IMAP_PASSWORD")
        self.folder = os.getenv("IMAP_FOLDER", "INBOX")
        self.use_ssl = os.getenv("IMAP_SSL", "true").lower() == "true"
        self.download_dir = Path(os.getenv("LINKEDIN_DATA_PATH", "./linkedin_exports/"))
        self.move_to = os.getenv("IMAP_MOVE_TO", "")  # optional folder to move processed emails

        self.download_dir.mkdir(parents=True, exist_ok=True)

    def _connect(self):
        if not self.username or not self.password:
            raise ValueError("IMAP_USERNAME/IMAP_PASSWORD missing in env")
        imap = imaplib.IMAP4_SSL(self.host, self.port) if self.use_ssl else imaplib.IMAP4(self.host, self.port)
        imap.login(self.username, self.password)
        return imap

    def _decode(self, s):
        if not s: return ""
        parts = decode_header(s)
        decoded = ""
        for text, enc in parts:
            if isinstance(text, bytes):
                decoded += text.decode(enc or "utf-8", errors="ignore")
            else:
                decoded += text
        return decoded

    def _is_linkedin_export(self, subject: str, from_addr: str) -> bool:
        subj = (subject or "").lower()
        sender = (from_addr or "").lower()
        return (
            "linkedin" in sender and
            any(k in subj for k in ["export", "analytics", "page analytics", "content", "posts", "demographics", "follower", "audience"])
        )

    def _is_digest(self, subject: str, from_addr: str) -> bool:
        subj = (subject or "").lower()
        sender = (from_addr or "").lower()
        return (
            "linkedin" in sender and
            any(k in subj for k in ["page update", "page analytics", "weekly update", "your page"])
        )

    def fetch_new_exports(self) -> List[Path]:
        imap = self._connect()
        try:
            imap.select(self.folder)
            status, data = imap.search(None, '(UNSEEN)')
            if status != "OK": return []

            new_files = []
            for num in data[0].split():
                status, msg_data = imap.fetch(num, '(RFC822)')
                if status != "OK":
                    continue

                msg = email.message_from_bytes(msg_data[0][1])
                subject = self._decode(msg.get("Subject"))
                from_addr = self._decode(msg.get("From"))

                if not self._is_linkedin_export(subject, from_addr):
                    continue

                saved_any = False
                for part in msg.walk():
                    if part.get_content_maintype() == 'multipart':
                        continue
                    filename = part.get_filename()
                    if not filename:
                        continue
                    fname_decoded = self._decode(filename)
                    if not fname_decoded.lower().endswith(('.csv', '.xls', '.xlsx', '.zip')):
                        continue

                    ts = datetime.now().strftime("%Y%m%d_%H%M%S")
                    safe_name = fname_decoded.replace('/', '_').replace('\\', '_')
                    out_path = self.download_dir / f"{ts}_{safe_name}"
                    with open(out_path, 'wb') as f:
                        f.write(part.get_payload(decode=True))
                    new_files.append(out_path)
                    saved_any = True

                # mark seen and optionally move
                imap.store(num, '+FLAGS', '\\Seen')
                if saved_any and self.move_to:
                    try:
                        imap.create(self.move_to)
                    except:
                        pass
                    imap.copy(num, self.move_to)
                    imap.store(num, '+FLAGS', '\\Deleted')

            imap.expunge()
            return new_files
        finally:
            try:
                imap.close()
            except:
                pass
            imap.logout()

    def fetch_digests(self) -> list[dict]:
        """
        Fetch LinkedIn Page admin digest emails (no attachments).
        Returns list of dicts: [{subject, from, date, body}, ...]
        """
        imap = self._connect()
        digests = []
        try:
            imap.select(self.folder)
            status, data = imap.search(None, '(UNSEEN)')
            if status != "OK":
                return []

            for num in data[0].split():
                status, msg_data = imap.fetch(num, '(RFC822)')
                if status != "OK":
                    continue

                msg = email.message_from_bytes(msg_data[0][1])
                subject = self._decode(msg.get("Subject"))
                from_addr = self._decode(msg.get("From"))

                if not self._is_digest(subject, from_addr):
                    continue

                # Skip if attachments exist (handled by fetch_new_exports)
                has_attachment = False
                for part in msg.walk():
                    if part.get_content_maintype() == 'multipart':
                        continue
                    if part.get_filename():
                        has_attachment = True
                        break
                if has_attachment:
                    continue

                # Extract body (prefer HTML)
                body = ""
                if msg.is_multipart():
                    for part in msg.walk():
                        ctype = part.get_content_type()
                        disp = str(part.get('Content-Disposition') or '')
                        if ctype in ["text/html", "text/plain"] and 'attachment' not in disp.lower():
                            try:
                                body = part.get_payload(decode=True).decode(part.get_content_charset() or 'utf-8', errors='ignore')
                                break
                            except:
                                continue
                else:
                    try:
                        body = msg.get_payload(decode=True).decode(msg.get_content_charset() or 'utf-8', errors='ignore')
                    except:
                        body = str(msg.get_payload())

                digests.append({
                    "subject": subject,
                    "from": from_addr,
                    "date": msg.get("Date"),
                    "body": body
                })

                imap.store(num, '+FLAGS', '\\Seen')

            imap.expunge()
            return digests
        finally:
            try:
                imap.close()
            except:
                pass
            imap.logout()