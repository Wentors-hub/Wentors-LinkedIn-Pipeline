import re

CANON_POST_TYPES = ['text', 'article', 'image', 'document', 'video']

def normalize_post_type(raw_type: str, content: str, url: str = '', title: str = '') -> str:
    raw = (raw_type or '').strip().lower()
    text = ' '.join([content or '', url or '', title or '']).lower()

    # “organic/sponsored/paid” are distribution, not type
    if raw in ['organic', 'sponsored', 'paid', 'boosted', 'promoted']:
        raw = ''

    # Heuristics
    if 'video' in raw or 'video' in text: return 'video'
    if any(k in raw for k in ['image','photo','picture']) or any(k in text for k in ['image','photo','picture','jpg','jpeg','png']): return 'image'
    if any(k in raw for k in ['document','pdf','doc']) or any(k in text for k in ['document','pdf','.pdf','.ppt','.pptx','.doc','.docx','.slideshare']): return 'document'
    if any(k in raw for k in ['article','link']) or '/pulse/' in text: return 'article'
    if 'text' in raw or 'status' in raw: return 'text'
    # Fallback
    return 'article' if 'http' in text else 'text'

def extract_hashtags(text: str) -> list:
    if not text: return []
    tags = re.findall(r'(?<!&)#([\w\d_]+)', text)
    uniq = list({f'#{t.strip()}' for t in tags if t.strip()})
    return uniq[:50]

def extract_mentions(text: str) -> list:
    if not text: return []
    m = re.findall(r'@([\w\.\-]+)', text)
    uniq = list({f'@{t.strip()}' for t in m if t.strip()})
    return uniq[:50]

def compute_ctr(clicks: int, impressions: int) -> float:
    return round((clicks / impressions) * 100.0, 4) if impressions and impressions > 0 else 0.0