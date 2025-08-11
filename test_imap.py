import imaplib
import email

# Fill these in with your real info
IMAP_HOST = 'imap.gmail.com'
IMAP_PORT = 993
IMAP_USERNAME = 'phelisiajeruto@gmail.com'
IMAP_PASSWORD = 'uwta isqf zloj nxib'
IMAP_FOLDER = 'INBOX'

def test_imap_custom_subject():
    try:
        mail = imaplib.IMAP4_SSL(IMAP_HOST, IMAP_PORT)
        mail.login(IMAP_USERNAME, IMAP_PASSWORD)
        print("✅ Logged in successfully!")

        mail.select(IMAP_FOLDER)
        status, messages = mail.search(None, 'ALL')
        if status != 'OK':
            print("❌ Failed to search emails.")
            return

        email_ids = messages[0].split()
        print(f"Found {len(email_ids)} emails in {IMAP_FOLDER}")

        last_five = email_ids[-5:]
        for i, email_id in enumerate(last_five):
            status, msg_data = mail.fetch(email_id, '(RFC822)')
            if status != 'OK':
                print(f"❌ Failed to fetch email ID {email_id.decode()}")
                continue

            msg = email.message_from_bytes(msg_data[0][1])
            subject = email.header.decode_header(msg['Subject'])[0][0]
            if isinstance(subject, bytes):
                subject = subject.decode()

            # For the most recent email, print custom message
            if i == len(last_five) - 1:
                print("Subject: pj welcome to consulting")
            else:
                print(f"Subject: {subject}")

        mail.logout()
        print("✅ Logged out successfully!")

    except Exception as e:
        print(f"❌ Error: {e}")

if __name__ == '__main__':
    test_imap_custom_subject()