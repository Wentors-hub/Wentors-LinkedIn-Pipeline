# email_digest_parser.py
import re

def _safe_int(s):
    try:
        if s is None: return 0
        s = str(s).replace(',', '').strip()
        return int(float(s))
    except:
        return 0

def _extract_numbers(text, label_patterns):
    for pat in label_patterns:
        m = re.search(pat, text, flags=re.I)
        if m:
            return _safe_int(m.group(1))
    return 0

def parse_page_digest(html_or_text: str) -> dict:
    # Strip HTML to text
    text = re.sub(r'<[^>]+>', ' ', html_or_text or '')
    text = re.sub(r'\s+', ' ', text).strip()

    followers = _extract_numbers(text, [r'([\d,\.]+)\s+followers', r'total followers[:\s]+([\d,\.]+)', r'followers[:\s]+([\d,\.]+)'])
    impressions = _extract_numbers(text, [r'([\d,\.]+)\s+impressions', r'update impressions[:\s]+([\d,\.]+)'])

    summary = {
        "followers_count": followers or 0,
        "impressions": impressions or 0,
        "unique_impressions": impressions or 0,
        "clicks": 0,
        "engagement_rate": 0.0,
        "reach": impressions or 0,
        "total_posts": 0,
        "avg_post_engagement": 0.0
    }
    return summary