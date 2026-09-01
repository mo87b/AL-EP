import os
import sys
import re
import json
import time
import shutil
import tempfile
import asyncio
import datetime
import hashlib
import subprocess
import urllib.parse
import xml.etree.ElementTree as ET
import email.utils
import httpx

# ─── Environment Configuration ──────────────────────────────────
TURSO_URL = os.environ.get("TURSO_URL", "")
TURSO_TOKEN = os.environ.get("TURSO_TOKEN", "")
PIXELDRAIN_API_KEY = os.environ.get("PIXELDRAIN_API_KEY", "")

GAS_PROXIES = []
for _k in ["GAS_PROXY_URL", "GAS_PROXY_URL_2", "GAS_PROXY_URL_3"]:
    _v = os.environ.get(_k, "").strip()
    if _v and _v not in GAS_PROXIES:
        GAS_PROXIES.append(_v)

_proxy_idx = 0
def get_ordered_proxies() -> list:
    global _proxy_idx
    if not GAS_PROXIES:
        return []
    n = len(GAS_PROXIES)
    start = _proxy_idx % n
    _proxy_idx += 1
    return [GAS_PROXIES[(start + i) % n] for i in range(n)]

SYNC_DAYS = int(os.environ.get("SYNC_DAYS", "12"))
SYNC_SECONDS = SYNC_DAYS * 24 * 60 * 60
MAX_DOWNLOADS_PER_RUN = int(os.environ.get("MAX_DOWNLOADS_PER_RUN", "5"))
TORRENT_DOWNLOAD_TIMEOUT = int(os.environ.get("TORRENT_DOWNLOAD_TIMEOUT", "300"))
MIN_TORRENT_SEEDERS = int(os.environ.get("MIN_TORRENT_SEEDERS", "7"))

NYAA_TRACKERS = [
    "http://nyaa.tracker.wf:7777/announce",
    "udp://open.stealth.si:80/announce",
    "udp://tracker.opentrackr.org:1337/announce",
    "udp://exodus.desync.com:6969/announce",
    "udp://tracker.torrent.eu.org:451/announce",
    "udp://tracker.tiny-vps.com:6969/announce",
    "udp://tracker.moeking.me:6969/announce",
    "udp://explodie.org:6969/announce",
]

def log_message(msg: str):
    ts = datetime.datetime.now().strftime("%Y-%m-%d %H:%M:%S")
    print(f"[{ts}] {msg}", flush=True)

# ─── Turso Database Helpers ─────────────────────────────────────
def _make_turso_args(args: list) -> list:
    turso_args = []
    for arg in (args or []):
        if arg is None:
            turso_args.append({"type": "null"})
        elif isinstance(arg, int):
            turso_args.append({"type": "integer", "value": str(arg)})
        elif isinstance(arg, float):
            turso_args.append({"type": "float", "value": arg})
        else:
            turso_args.append({"type": "text", "value": str(arg)})
    return turso_args

def _parse_turso_result(exec_result: dict) -> list:
    cols = [col["name"] for col in exec_result.get("cols", [])]
    rows = exec_result.get("rows", [])
    parsed_rows = []
    for row in rows:
        row_dict = {}
        for i, cell in enumerate(row):
            val_type = cell.get("type")
            val = cell.get("value")
            if val_type == "null":
                row_dict[cols[i]] = None
            elif val_type == "integer":
                row_dict[cols[i]] = int(val)
            elif val_type == "float":
                row_dict[cols[i]] = float(val)
            else:
                row_dict[cols[i]] = str(val)
        parsed_rows.append(row_dict)
    return parsed_rows

async def execute_sql(sql: str, args: list = None) -> list:
    body = {
        "requests": [
            {"type": "execute", "stmt": {"sql": sql, "args": _make_turso_args(args)}},
            {"type": "close"}
        ]
    }
    headers = {"Authorization": f"Bearer {TURSO_TOKEN}", "Content-Type": "application/json"}
    try:
        async with httpx.AsyncClient(timeout=25.0) as client:
            r = await client.post(f"{TURSO_URL}/v2/pipeline", headers=headers, json=body)
            if r.status_code != 200:
                log_message(f"DB error ({r.status_code})")
                return []
            res = r.json()
            first_res = res.get("results", [{}])[0]
            if first_res.get("type") == "error":
                err_msg = first_res.get("error", {}).get("message", "Unknown DB error")
                log_message(f"DB execute error: {err_msg}")
                return []
            exec_result = first_res.get("response", {}).get("result", {})
            return _parse_turso_result(exec_result)
    except Exception as e:
        log_message(f"DB exception: {e}")
        return []

async def execute_sql_batch(statements: list) -> list:
    if not statements:
        return []
    requests = []
    for sql, args in statements:
        requests.append({"type": "execute", "stmt": {"sql": sql, "args": _make_turso_args(args)}})
    requests.append({"type": "close"})

    headers = {"Authorization": f"Bearer {TURSO_TOKEN}", "Content-Type": "application/json"}
    async with httpx.AsyncClient(timeout=30.0) as client:
        r = await client.post(f"{TURSO_URL}/v2/pipeline", headers=headers, json={"requests": requests})
        if r.status_code != 200:
            log_message(f"DB batch error ({r.status_code})")
            return []
        res = r.json()
        results = []
        for i, result_obj in enumerate(res.get("results", [])):
            resp = result_obj.get("response", {})
            if resp.get("type") == "execute":
                results.append(_parse_turso_result(resp.get("result", {})))
        return results

# ─── Database Maintenance & Blacklist ───────────────────────────
async def ensure_database_schema():
    await execute_sql("""
        CREATE TABLE IF NOT EXISTS anime_blacklist (
            anilist_id INTEGER PRIMARY KEY,
            title_romaji TEXT,
            reason TEXT,
            blacklisted_at INTEGER
        )
    """)
    for col in ["is_multi_audio INTEGER DEFAULT 0", "audio_score INTEGER DEFAULT 0", "erai_title TEXT", 
                "backup_720_url TEXT", "backup_720_id TEXT", "backup_480_url TEXT", "backup_480_id TEXT",
                "pending_review_until INTEGER DEFAULT 0"]:
        try:
            col_name = col.split()[0]
            await execute_sql(f"ALTER TABLE episodes ADD COLUMN {col}")
        except Exception:
            pass

    # Self-healing: Reset any stuck processing episodes from crashed runs
    try:
        affected = await execute_sql("UPDATE episodes SET status = 'pending' WHERE status = 'processing'")
        if affected:
            log_message("Reset stuck 'processing' episodes back to 'pending'.")
    except Exception:
        pass

    # Auto-purge Ecchi / Hentai into blacklist and remove from database
    try:
        now_ts = int(time.time())
        await execute_sql("""
            INSERT INTO anime_blacklist (anilist_id, title_romaji, reason, blacklisted_at)
            SELECT anilist_id, title_romaji, 'ecchi_hentai_genre', ?
            FROM anime
            WHERE lower(genres) LIKE '%ecchi%' OR lower(genres) LIKE '%hentai%'
            ON CONFLICT(anilist_id) DO UPDATE SET reason = 'ecchi_hentai_genre'
        """, [now_ts])

        await execute_sql("""
            DELETE FROM episodes
            WHERE anime_id IN (
                SELECT id FROM anime
                WHERE lower(genres) LIKE '%ecchi%' OR lower(genres) LIKE '%hentai%'
            )
        """)

        await execute_sql("""
            DELETE FROM anime
            WHERE lower(genres) LIKE '%ecchi%' OR lower(genres) LIKE '%hentai%'
        """)
    except Exception:
        pass

async def get_blacklisted_ids() -> set:
    rows = await execute_sql("SELECT anilist_id FROM anime_blacklist")
    return {r["anilist_id"] for r in rows} if rows else set()

async def blacklist_anime(anime_id: int, anilist_id: int, title_romaji: str, reason: str):
    now_ts = int(time.time())
    await execute_sql("""
        INSERT INTO anime_blacklist (anilist_id, title_romaji, reason, blacklisted_at)
        VALUES (?, ?, ?, ?)
        ON CONFLICT(anilist_id) DO UPDATE SET
            title_romaji = excluded.title_romaji,
            reason = excluded.reason,
            blacklisted_at = excluded.blacklisted_at
    """, [anilist_id, title_romaji, reason, now_ts])
    await execute_sql("DELETE FROM episodes WHERE anime_id = ?", [anime_id])
    await execute_sql("DELETE FROM anime WHERE id = ?", [anime_id])
    log_message(f"Blacklisted anime: {title_romaji} (Reason: {reason})")

# ─── Title & Episode Parsing Functions ─────────────────────────
def clean_title(title: str) -> str:
    if not title or not isinstance(title, str):
        return ""
    title = re.sub(r'\(.*?\)', '', title)
    title = re.sub(r'\[.*?\]', '', title)
    title = re.sub(r'[:\\/*?"<>|]', ' ', title)
    title = re.sub(r'[^a-zA-Z0-9\s\-\'\.]', '', title)
    return re.sub(r'\s+', ' ', title).strip()

def clean_and_strip(title: str) -> str:
    t = clean_title(title)
    t = re.sub(r'\b\d{4}\b', ' ', t)
    t = re.sub(r'\b\d+(st|nd|rd|th)\s+season\b', '', t, flags=re.IGNORECASE)
    t = re.sub(r'\bseason\s+\d+\b', '', t, flags=re.IGNORECASE)
    t = re.sub(r'\bcour\s+\d+\b', '', t, flags=re.IGNORECASE)
    t = re.sub(r'\bs\d+\b', '', t, flags=re.IGNORECASE)
    return re.sub(r'\s+', ' ', t).strip()

def parse_erai_anime_title(filename: str) -> str:
    if not filename or not isinstance(filename, str):
        return ""
    m = re.match(r'^\[Erai-raws\]\s+(.*?)\s+-\s+\d+', filename)
    return m.group(1).strip() if m else ""

def get_part_number(title: str) -> int:
    if not title or not isinstance(title, str):
        return 0
    t_lower = title.lower()
    
    m = re.search(r'\b(?:part|cour|pt)\s*[-_.: ]*\s*(iv|iii|ii|i)\b', t_lower)
    if m:
        roman_map = {'i': 1, 'ii': 2, 'iii': 3, 'iv': 4}
        return roman_map.get(m.group(1), 0)
    
    m = re.search(r'\b(\d+)(st|nd|rd|th)\s+(?:part|cour|pt)\b', t_lower)
    if m:
        return int(m.group(1))

    m = re.search(r'\b(?:part|cour|pt)\s*[-_.: ]*\s*0*(\d+)\b', t_lower)
    if m:
        return int(m.group(1))

    return 0

def get_season_number(title: str) -> int:
    if not title or not isinstance(title, str):
        return 1
    title_lower = title.lower()
    title_lower = re.sub(r'^\[erai-raws\]\s+', '', title_lower)
    title_lower = re.split(r'\s+-\s+\d+', title_lower)[0]
    
    m = re.search(r'\bs(\d+)e(\d+)\b', title_lower)
    if m:
        return int(m.group(1))

    m = re.search(r'\bs(?:eason)?\s*0*(\d+)\b', title_lower)
    if m:
        return int(m.group(1))
        
    m = re.search(r'\b(\d+)(st|nd|rd|th)(?:\s+season)?\b', title_lower)
    if m:
        return int(m.group(1))
        
    m = re.search(r'\b(?:part|cour)\s*0*(\d+)\b', title_lower)
    if m:
        return int(m.group(1))

    clean_no_ver = re.sub(r'\bv\d+\b', '', title_lower)
    if re.search(r'\bii\b$', clean_no_ver) or re.search(r'\bii\b(?=\s)', clean_no_ver):
        return 2
    if re.search(r'\biii\b$', clean_no_ver) or re.search(r'\biii\b(?=\s)', clean_no_ver):
        return 3
    if re.search(r'\biv\b$', clean_no_ver) or re.search(r'\biv\b(?=\s)', clean_no_ver):
        return 4
    if (re.search(r'\bv\b$', clean_no_ver) or re.search(r'\bv\b(?=\s)', clean_no_ver)) and not re.search(r'\b(1080p|720p|480p|2160p|mkv|mp4|v)\s+v\b', title_lower):
        return 5
 
    clean_end = re.sub(r'[^a-z0-9\s]', '', title_lower).strip()
    m = re.search(r'\s+(\d+)$', clean_end)
    if m:
        num = int(m.group(1))
        if num < 10:
            return num
    return 1

def is_blacklisted_platform(title: str) -> bool:
    if not title or not isinstance(title, str):
        return False
    return bool(re.search(r'\b(nf|netflix|iq|iqiyi)\b', title.lower()))

def get_audio_score(title: str) -> int:
    if not title or not isinstance(title, str):
        return 0
    t_lower = title.lower()
    if re.search(r'\bmulti[- ]audio\b|multiaudio|\bmulti\s+aac\b', t_lower):
        return 2
    if re.search(r'\bdual[- ]audio\b|dualaudio|\bdual\b', t_lower):
        return 1
    return 0

def is_multi_audio_torrent(title: str) -> bool:
    return get_audio_score(title) > 0

def get_platform_score(title: str) -> int:
    if not title or not isinstance(title, str):
        return 0
    t_lower = title.lower()
    if re.search(r'\b(cr|crunchyroll|amzn|amazon|shahid|starzplay|starz|adn)\b', t_lower):
        return 3
    elif re.search(r'\b(nf|netflix)\b', t_lower):
        return 2
    elif re.search(r'\b(bili|bilibili|iq|iqiyi|disney|hulu|abema|baha|bahamut|ani-one|anione|muse|yt|youtube|wetv)\b', t_lower):
        return 1
    return 0

def get_quality_weight(title: str) -> int:
    if not title or not isinstance(title, str):
        return 0
    t_lower = title.lower()
    if "1080" in t_lower:
        return 3
    elif "720" in t_lower:
        return 2
    elif "480" in t_lower:
        return 1
    return 0

def get_source_weight(title: str) -> int:
    if not title or not isinstance(title, str):
        return 0
    t_lower = title.lower()
    if "web-dl" in t_lower or "webdl" in t_lower:
        return 2
    elif "webrip" in t_lower:
        return 1
    return 0

SEASON_STOPWORDS = {
    "1st", "2nd", "3rd", "4th", "5th", "6th", "7th", "8th", "9th", "10th",
    "season", "cour", "part", "s1", "s2", "s3", "s4", "s5", "s6", "s7", "s8", "s9",
    "tv", "bd", "bluray", "blu-ray", "dvd", "web", "web-dl", "webrip", "hdtv", 
    "uncensored", "uncut", "censored", "dual", "multi", "audio", "sub", "subs", 
    "subtitle", "subtitles", "dub", "dubs", "dubbed", "v0", "v1", "v2", "v3", 
    "batch", "reupload", "re-upload", "remux", "hevc", "x264", "x265", "h264", "h265", 
    "10bit", "10bits", "8bit", "8bits", "version", "edit", "specials", "special", "mkv", "mp4", "avi", "webm",
    "1080p", "720p", "480p", "1080", "720", "480", "2160p", "2160", "4k", "5k", "8k",
    "aac2", "aac", "aac5", "ddp2", "ddp5", "ddp", "dts", "ac3", "flac", "avc", "av1", "av01",
    "hdr", "hdr10", "hdr10plus", "sdr", "atmos", "hi10p", "hi10",
    "amzn", "cr", "cru", "nf", "nflx", "netflix", "hulu", "dnp", "disney", "bilibili", "bili", "bsite", "yt", "youtube", "adn", "wetv", "iq", "iqiyi", "mgtv", "youku", "abema", "baha", "bahamut",
    "varyg", "subsplease", "erai-raws", "erai", "judas", "ember", "asw", "kaede", "horriblesubs", "horrible", "sirius", "pas", "commie",
    "tsundere", "raws", "rapta", "repack", "vostfr", "dl", "ona", "ova", "movie", "weekly",
    "eng", "english", "jap", "japanese", "ara", "arabic", "multi-subs", "multisubs", "multisub", "multi-sub",
    "gradation"
}

def get_clean_words(title: str) -> list:
    title_lower = title.lower()
    title_no_se = re.sub(r'\b(s\d+e\d+|s\d+|e\d+)\b', ' ', title_lower)
    title_no_num = re.sub(r'\b\d+\b', ' ', title_no_se)
    clean_t = title_no_num.replace('.', ' ').replace('-', ' ').replace("'", "")
    clean_t = re.sub(r'[^a-zA-Z0-9\s]', ' ', clean_t)
    words = clean_t.split()
    if not words:
        clean_with_num = title_no_se.replace('.', ' ').replace('-', ' ').replace("'", "")
        clean_with_num = re.sub(r'[^a-zA-Z0-9\s]', ' ', clean_with_num)
        words = clean_with_num.split()
    
    particles = {
        "no", "to", "in", "of", "a", "an", "the", "is", "at", "by", "on", 
        "and", "or", "for", "with", "wa", "ga", "wo", "ni", "de", "ka", "mo"
    }
    
    filtered = []
    for w in words:
        w_stripped = w.strip("-'")
        if not w_stripped or w_stripped in SEASON_STOPWORDS or w_stripped in particles:
            continue
        if len(w_stripped) >= 2 or (len(w_stripped) == 1 and w_stripped.isalnum()):
            filtered.append(w_stripped)
    return filtered

def is_matching_torrent(torrent_title: str, romaji: str, english: str, ep: int, synonyms: list = None, is_special: bool = False) -> bool:
    if not torrent_title or not romaji:
        return False
    t_lower = torrent_title.lower()
    synonyms = synonyms or []

    # 1. Episode matching check
    m_ep = re.search(r'\b(?:s\d+)?e(\d+)\b', t_lower)
    if m_ep:
        if int(m_ep.group(1)) != ep:
            return False
    else:
        bypass_ep_check = False
        if is_special and ep == 1:
            clean_title_for_ep = re.sub(r'\b(1080p|720p|480p|2160p|1080|720|480|2160|3d|4k|5k|8k|x264|x265|h264|h265|10bit|8bit|v\d+)\b', '', t_lower)
            other_ep_match = re.search(r'\b(?:ep|episode|ep\.|sp|special)?\s*0*([2-9]|\d{2,})\b', clean_title_for_ep)
            if not other_ep_match:
                bypass_ep_check = True
                
        if not bypass_ep_check:
            ep_pattern = re.compile(rf'\b0*{ep}\b')
            if not ep_pattern.search(t_lower):
                return False

    # 2. Canonical Season & Part Enforcement
    torrent_season = get_season_number(torrent_title)
    clean_romaji = clean_title(romaji)
    clean_english = clean_title(english) if english else ""

    target_season = get_season_number(clean_romaji)
    if target_season == 1 and clean_english:
        eng_s = get_season_number(clean_english)
        if eng_s > 1:
            target_season = eng_s

    if torrent_season != target_season:
        return False

    target_part = get_part_number(clean_romaji) or (get_part_number(clean_english) if english else 0)
    torrent_part = get_part_number(torrent_title)
    if torrent_part != target_part:
        return False

    # 3. Filter synonyms: remove invalid non-latin remnants that produce 1-letter false positives (like 'X')
    valid_synonyms = []
    for s in synonyms:
        if not s or not isinstance(s, str):
            continue
        if re.search(r'[\u3040-\u30ff\u3400-\u4dbf\u4e00-\u9fff]', s):
            c_words = get_clean_words(clean_title(s))
            if len(c_words) < 2 or all(len(w) < 3 for w in c_words):
                continue
        valid_synonyms.append(s)

    # Title Matching with ratio, word-boundary, and delimiter support
    def is_title_match(anime_title: str, torrent_title_lower: str) -> bool:
        if not anime_title:
            return False
            
        def check_match(raw_title_str: str) -> bool:
            clean_t = clean_title(raw_title_str)
            words = get_clean_words(clean_t)
            if not words:
                return False
            # Use exact word boundary matching (\bword\b) to avoid matching single letters inside unrelated words
            matching_words = [w for w in words if re.search(rf'\b{re.escape(w)}\b', torrent_title_lower)]
            ratio = len(matching_words) / len(words)
            
            if len(words) <= 2:
                return len(matching_words) == len(words)
            if len(words) == 3:
                return len(matching_words) >= 2
            return ratio >= 0.75

        if check_match(anime_title):
            return True

        delimiters = [':', '-']
        for delim in delimiters:
            if delim in anime_title:
                parts = anime_title.split(delim)
                for part in parts:
                    part_stripped = part.strip()
                    if len(get_clean_words(clean_title(part_stripped))) >= 2:
                        if check_match(part_stripped):
                            return True
        return False

    romaji_match = is_title_match(romaji, t_lower)
    eng_match = is_title_match(english, t_lower) if english else False
    syn_match = any(is_title_match(syn, t_lower) for syn in valid_synonyms)

    if not romaji_match and not eng_match and not syn_match:
        return False

    # 4. Extra words check (with Japanese concatenation check)
    clean_matched_words = get_clean_words(romaji)
    is_trusted_group = bool(re.search(r'\[?(erai[-_ ]?raws|toonshub)\]?', t_lower)) and len(clean_matched_words) >= 2
    if not is_trusted_group:
        torrent_clean = clean_title(torrent_title)
        torrent_words = get_clean_words(torrent_clean)
        
        anime_words = set(get_clean_words(romaji) + (get_clean_words(english) if english else []))
        for syn in valid_synonyms:
            if syn:
                anime_words.update(get_clean_words(syn))
                
        extra_words = []
        concat_parts = set()
        for i in range(len(torrent_words) - 1):
            pair_word = torrent_words[i] + torrent_words[i+1]
            if pair_word in anime_words:
                concat_parts.add(torrent_words[i])
                concat_parts.add(torrent_words[i+1])

        for w in torrent_words:
            if w in anime_words or w in concat_parts:
                continue
            is_concat = False
            for w1 in anime_words:
                if len(w1) >= 3 and w.startswith(w1) and w[len(w1):] in anime_words:
                    is_concat = True
                    break
            if not is_concat:
                extra_words.append(w)
                
        if extra_words:
            return False

    # 5. Source check: MUST be Multi-Sub release
    is_multi_sub = bool(re.search(
        r'\b(multi|m)\s*[-_:]?\s*subs?\b|'
        r'multisubs?|'
        r'multiple\s+subtitles?|'
        r'multiple\s+subs?\b|'
        r'\[multi[-_ ]?subs?\]|'
        r'\[multiple[-_ ]?subtitles?\]',
        t_lower
    ))
    return is_multi_sub

def _title_segments(title: str) -> list:
    """Colon/dash-separated title parts that can stand alone in search."""
    segs = []
    if not title or not isinstance(title, str):
        return segs
    for part in re.split(r':|\s+-\s+', title):
        cleaned = clean_and_strip(part)
        if cleaned and len(cleaned.split()) >= 2 and cleaned not in segs:
            segs.append(cleaned)
    return segs[:4]

def get_search_queries(romaji: str, english: str, ep: int, synonyms: list = None, is_special: bool = False, erai_title: str = None) -> list:
    queries = []
    ep_str = f"{ep:02d}"
    synonyms = synonyms or []

    r_base = clean_and_strip(romaji)
    e_base = clean_and_strip(english) if english else ""
    
    r_super = re.sub(r'[\-\.]', ' ', r_base).replace("'", "")
    r_super = re.sub(r'\s+', ' ', r_super).strip()
    
    e_super = ""
    if e_base:
        e_super = re.sub(r'[\-\.]', ' ', e_base).replace("'", "")
        e_super = re.sub(r'\s+', ' ', e_super).strip()
    
    search_bases = []
    if erai_title:
        search_bases.append(clean_and_strip(erai_title))
    for source_title in (romaji, english):
        for seg in _title_segments(source_title):
            if seg not in search_bases:
                search_bases.append(seg)
    search_bases.extend([r_base, e_base])
    if r_super and r_super not in search_bases:
        search_bases.append(r_super)
    if e_super and e_super not in search_bases:
        search_bases.append(e_super)
        
    # Japanese suffix / hyphen variations (e.g. Tenkousaki -> Tenkou-saki / Tenkou saki)
    COMMON_SUFFIXES = ["saki", "tabi", "gumi", "jima", "bashi", "mura", "kan", "sou", "ken", "chou"]
    for title_base in [r_base] + synonyms:
        if not title_base:
            continue
        c_words = clean_and_strip(title_base).split()
        for i, w in enumerate(c_words[:3]):
            w_lower = w.lower()
            if "-" in w:
                unhyphen = w.replace("-", "")
                spaced = w.replace("-", " ")
                v1 = " ".join(c_words[:i] + [unhyphen] + c_words[i+1:])
                v2 = " ".join(c_words[:i] + [spaced] + c_words[i+1:])
                for var in (v1, v2):
                    if var and var not in search_bases:
                        search_bases.append(var)
            else:
                for sfx in COMMON_SUFFIXES:
                    if w_lower.endswith(sfx) and len(w_lower) > len(sfx) + 2:
                        pfx = w[:-len(sfx)]
                        hyphen_var = f"{pfx}-{sfx}"
                        space_var = f"{pfx} {sfx}"
                        v1 = " ".join(c_words[:i] + [hyphen_var] + c_words[i+1:])
                        v2 = " ".join(c_words[:i] + [space_var] + c_words[i+1:])
                        for var in (v1, v2):
                            if var and var not in search_bases:
                                search_bases.append(var)
        
    for syn in synonyms:
        cleaned_syn = clean_and_strip(syn)
        if cleaned_syn and cleaned_syn not in search_bases:
            search_bases.append(cleaned_syn)
            
    for base in search_bases:
        if not base:
            continue
        queries.append(f'{base} "{ep_str}"')
        queries.append(f'{base} {ep_str}')
        if is_special and ep == 1:
            queries.append(base)
        
        words = base.split()
        if len(words) > 3:
            short = " ".join(words[:3])
            queries.append(f'{short} "{ep_str}"')
            queries.append(f'{short} {ep_str}')
            if is_special and ep == 1:
                queries.append(short)
            
    for base_romaji in [r_base] + synonyms:
        if not base_romaji:
            continue
        base_romaji_clean = clean_and_strip(base_romaji)
        if not base_romaji_clean:
            continue
            
        r_o = re.sub(r'\bwo\b', 'o', base_romaji_clean, flags=re.IGNORECASE)
        r_wo = re.sub(r'\bo\b', 'wo', base_romaji_clean, flags=re.IGNORECASE)
        for var in [r_o, r_wo]:
            if var != base_romaji_clean:
                queries.append(f'{var} "{ep_str}"')
                queries.append(f'{var} {ep_str}')
                words = var.split()
                if len(words) > 3:
                    short_var = " ".join(words[:3])
                    queries.append(f'{short_var} "{ep_str}"')
                    queries.append(f'{short_var} {ep_str}')
                    
        r_words = base_romaji_clean.split()
        if len(r_words) >= 2:
            merged_first_two = r_words[0] + r_words[1]
            rest = " ".join(r_words[2:])
            var_merged = f"{merged_first_two} {rest}".strip()
            queries.append(f'{var_merged} "{ep_str}"')
            queries.append(f'{var_merged} {ep_str}')
            queries.append(f'{merged_first_two} "{ep_str}"')
            queries.append(f'{merged_first_two} {ep_str}')
            var_merged_o = re.sub(r'\bwo\b', 'o', var_merged, flags=re.IGNORECASE)
            if var_merged_o != var_merged:
                queries.append(f'{var_merged_o} "{ep_str}"')
                queries.append(f'{var_merged_o} {ep_str}')

    return list(dict.fromkeys(queries))

def extract_info_hash(payload: bytes) -> str:
    """SHA-1 of the raw bencoded 'info' dictionary of a .torrent file."""
    try:
        data = payload

        def read_str(i):
            colon = data.index(b":", i)
            length = int(data[i:colon])
            start = colon + 1
            return start, start + length

        def skip(i):
            c = data[i:i+1]
            if c == b"i":
                end = data.index(b"e", i)
                return end + 1
            if c in (b"d", b"l"):
                i += 1
                is_dict = c == b"d"
                while data[i:i+1] != b"e":
                    if is_dict:
                        _, i = read_str(i)
                    i = skip(i)
                return i + 1
            _, end = read_str(i)
            return end

        if data[:1] != b"d":
            return None
        i = 1
        while data[i:i+1] != b"e":
            ks, ke = read_str(i)
            key = data[ks:ke]
            val_start = ke
            val_end = skip(val_start)
            if key == b"info":
                return hashlib.sha1(data[val_start:val_end]).hexdigest()
            i = val_end
    except Exception:
        return None
    return None

# ─── Nyaa Search & Proxy Integration ──────────────────────────
async def search_nyaa_rss(query: str, romaji: str, english: str, ep: int, synonyms: list = None, is_special: bool = False) -> tuple:
    """Returns (results, diagnostic_note). Tries available GAS proxies with automatic failover."""
    encoded_query = urllib.parse.quote(query)
    headers = {"User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36"}
    tag = query[:40].replace("\n", " ")
    
    proxies = get_ordered_proxies()
    if not proxies:
        return [], f"'{tag}' no GAS proxies configured"

    last_err = ""
    transport = httpx.AsyncHTTPTransport(retries=2)
    for proxy_base in proxies:
        url = f"{proxy_base}?q={encoded_query}"
        try:
            async with httpx.AsyncClient(transport=transport, timeout=20.0, headers=headers, follow_redirects=True) as client:
                r = await client.get(url)
                if r.status_code != 200:
                    last_err = f"'{tag}' proxy HTTP {r.status_code}"
                    continue

                raw_items = []
                text = r.text.strip()
                if text.startswith("{"):
                    try:
                        data = r.json()
                    except Exception:
                        last_err = f"'{tag}' invalid JSON body"
                        continue
                    payload = data.get("data")
                    if not isinstance(payload, list):
                        last_err = f"'{tag}' proxy error payload ({data.get('error') or data.get('status')})"
                        continue
                    for item in payload:
                        raw_items.append({
                            "title": item.get("title", ""),
                            "torrent": item.get("torrent", ""),
                            "seeders": int(item.get("seeders") or 0),
                            "pub_date": int(item.get("pub_date") or item.get("timestamp") or 0)
                        })
                elif "<rss" in text or "<item" in text:
                    try:
                        root = ET.fromstring(r.content)
                    except ET.ParseError:
                        last_err = f"'{tag}' unparsable XML body"
                        continue
                    items = root.findall(".//item")
                    for item in items:
                        title_el = item.find("title")
                        link_el = item.find("link")
                        pub_el = item.find("pubDate")
                        title = title_el.text if title_el is not None else ""
                        torrent_url = link_el.text if link_el is not None else ""
                        pub_date_ts = 0
                        if pub_el is not None and pub_el.text:
                            try:
                                pub_date_ts = int(email.utils.parsedate_to_datetime(pub_el.text).timestamp())
                            except Exception:
                                pub_date_ts = 0
                        seeders = 0
                        for child in item:
                            if child.tag.endswith("seeders"):
                                seeders = int(child.text or 0) if child.text and child.text.isdigit() else 0
                                break
                        raw_items.append({
                            "title": title,
                            "torrent": torrent_url,
                            "seeders": seeders,
                            "pub_date": pub_date_ts
                        })
                else:
                    body_head = text.strip()[:50].replace("\n", " ")
                    last_err = f"'{tag}' unexpected body: {body_head!r}"
                    continue

                if not raw_items:
                    return [], f"'{tag}' raw=0"

                results = []
                for item in raw_items:
                    t = item["title"]
                    torrent_url = item["torrent"]
                    seeders = item["seeders"]
                    pub_date = item.get("pub_date", 0)
                    if not t or not torrent_url:
                        continue

                    if is_matching_torrent(t, romaji, english, ep, synonyms=synonyms, is_special=is_special):
                        results.append({
                            "title": t,
                            "magnet": torrent_url,
                            "seeders": seeders,
                            "pub_date": pub_date
                        })
                if results:
                    return results, ""
                return [], f"'{tag}' raw={len(raw_items)} matched=0"
        except Exception as e:
            last_err = f"'{tag}' {type(e).__name__}"
            continue
            
    return [], last_err or f"'{tag}' all proxies failed"

# ─── aria2c Downloader & Pixeldrain Uploader ──────────────────
def is_valid_torrent_data(data: bytes) -> bool:
    """Verifies that bytes represent a valid bencoded torrent file (starts with 'd' and is not HTML)."""
    if not data or len(data) < 50:
        return False
    data_start = data[:100].lower()
    if data_start.startswith(b"<!doctype") or b"<html" in data_start or b"<head" in data_start:
        return False
    return data.startswith(b"d") and (b"announce" in data or b"info" in data)

def download_torrent(torrent_source: str, torrent_title: str) -> tuple:
    download_dir = tempfile.mkdtemp(prefix="anime_")
    torrent_file_path = os.path.join(download_dir, "download.torrent")
    raw_payload = None
    torrent_input = torrent_source

    # 1. Download .torrent file through available Google Apps Script Proxies with failover
    if torrent_source.startswith("http"):
        sync_transport = httpx.HTTPTransport(retries=2)
        for proxy_base in get_ordered_proxies():
            gas_url = f"{proxy_base}?mode=torrent&url={urllib.parse.quote(torrent_source)}"
            try:
                with httpx.Client(transport=sync_transport, timeout=30.0) as client:
                    r = client.get(gas_url)
                    if r.status_code == 200:
                        data = r.json()
                        if data.get("status") == 200 and data.get("data"):
                            import base64
                            raw_bytes = base64.b64decode(data["data"])
                            if is_valid_torrent_data(raw_bytes):
                                with open(torrent_file_path, "wb") as f:
                                    f.write(raw_bytes)
                                raw_payload = raw_bytes
                                torrent_input = torrent_file_path
                                break
            except Exception:
                continue
    else:
        torrent_input = torrent_source

    trackers_arg = ",".join(NYAA_TRACKERS)
    cmd = [
        "aria2c", torrent_input,
        f"--dir={download_dir}",
        "--seed-time=0",
        "--bt-stop-timeout=120",
        "--file-allocation=none",
        "--enable-dht=true",
        "--enable-peer-exchange=true",
        "--bt-enable-lpd=true",
        "--bt-max-peers=100",
        f"--bt-tracker={trackers_arg}",
        "--max-connection-per-server=16",
        "--summary-interval=10",
        "--allow-overwrite=true",
    ]

    log_message(f"Starting download: {torrent_title}")
    proc = subprocess.run(cmd, timeout=TORRENT_DOWNLOAD_TIMEOUT, capture_output=True, text=True)
    if proc.returncode != 0:
        shutil.rmtree(download_dir, ignore_errors=True)
        raise RuntimeError(f"aria2c failed with code {proc.returncode}")

    video_files = []
    for root, _, files in os.walk(download_dir):
        for f in files:
            if f.endswith((".mkv", ".mp4", ".avi", ".webm")) and not f.endswith((".aria2", ".torrent")):
                fp = os.path.join(root, f)
                video_files.append((fp, f, os.path.getsize(fp)))

    if not video_files:
        shutil.rmtree(download_dir, ignore_errors=True)
        raise RuntimeError("No video file found in downloaded torrent!")

    video_files.sort(key=lambda x: x[2], reverse=True)
    best_file = video_files[0]
    info_hash = extract_info_hash(raw_payload) if raw_payload else None
    return download_dir, best_file[0], best_file[1], best_file[2], info_hash

def upload_pixeldrain(file_path: str, filename: str) -> dict:
    url = f"https://pixeldrain.com/api/file/{urllib.parse.quote(filename)}"
    auth = ("", PIXELDRAIN_API_KEY) if PIXELDRAIN_API_KEY else None

    log_message(f"Uploading {filename}...")
    with open(file_path, "rb") as f:
        with httpx.Client(timeout=300.0) as client:
            r = client.put(url, content=f.read(), auth=auth)
            if r.status_code in [200, 201]:
                file_id = r.json().get("id")
                return {"id": file_id, "url": f"https://pixeldrain.com/api/file/{file_id}"}
            raise RuntimeError(f"Pixeldrain upload failed (HTTP {r.status_code}): {r.text}")

def delete_from_pixeldrain(file_id: str) -> bool:
    if not file_id or not PIXELDRAIN_API_KEY:
        return False
    try:
        url = f"https://pixeldrain.com/api/file/{file_id}"
        with httpx.Client(timeout=15.0) as client:
            r = client.delete(url, auth=("", PIXELDRAIN_API_KEY))
            return r.status_code == 200
    except Exception:
        return False

async def cleanup_pixeldrain_duplicates():
    if not PIXELDRAIN_API_KEY:
        return
    try:
        # Protect active IDs currently used in the database
        db_rows = await execute_sql("""
            SELECT pixeldrain_id, pixeldrain_1080_id, backup_720_id, backup_480_id 
            FROM episodes 
            WHERE status = 'ready'
        """)
        active_ids = set()
        for r in db_rows:
            for k in ["pixeldrain_id", "pixeldrain_1080_id", "backup_720_id", "backup_480_id"]:
                if r.get(k):
                    active_ids.add(r[k])

        url = "https://pixeldrain.com/api/user/files"
        async with httpx.AsyncClient(timeout=30.0) as client:
            r = await client.get(url, auth=("", PIXELDRAIN_API_KEY))
            if r.status_code == 200:
                files = r.json().get("files", [])
                by_name = {}
                for f in files:
                    name = f.get("name", "")
                    fid = f.get("id")
                    if name and fid:
                        by_name.setdefault(name, []).append(f)
                
                to_delete = []
                saved_bytes = 0
                for name, flist in by_name.items():
                    if len(flist) > 1:
                        keep_ids = {f["id"] for f in flist if f["id"] in active_ids}
                        if not keep_ids:
                            flist.sort(key=lambda x: x.get("date_upload", ""), reverse=True)
                            keep_ids = {flist[0]["id"]}
                        for dup in flist:
                            if dup["id"] not in keep_ids:
                                to_delete.append(dup["id"])
                                saved_bytes += dup.get("size", 0)
                
                if to_delete:
                    for fid in to_delete:
                        await client.delete(f"https://pixeldrain.com/api/file/{fid}", auth=("", PIXELDRAIN_API_KEY))
                    log_message(f"Storage Cleanup: Purged {len(to_delete)} duplicate files ({saved_bytes / 1073741824:.2f} GB reclaimed).")
    except Exception:
        pass

# ─── AniList Schedule Syncing ──────────────────────────────────
ANILIST_SCHEDULE_QUERY = """
query ($page: Int, $airingAt_greater: Int, $airingAt_lesser: Int) {
  Page(page: $page, perPage: 50) {
    pageInfo { hasNextPage }
    airingSchedules(airingAt_greater: $airingAt_greater, airingAt_lesser: $airingAt_lesser, sort: TIME_DESC) {
      episode
      airingAt
      media {
        id
        title { romaji english native }
        synonyms
        episodes
        status
        coverImage { large }
        bannerImage
        description
        genres
        season
        seasonYear
        format
        startDate { year }
      }
    }
  }
}
"""

async def sync_anilist_schedule():
    log_message("Syncing schedule...")
    now = int(time.time())
    twelve_days_ago = now - SYNC_SECONDS
    blacklisted = await get_blacklisted_ids()
    page = 1
    has_next = True

    while has_next and page <= 5:
        try:
            async with httpx.AsyncClient(timeout=25.0) as client:
                r = await client.post("https://graphql.anilist.co", json={
                    "query": ANILIST_SCHEDULE_QUERY,
                    "variables": {"page": page, "airingAt_greater": twelve_days_ago, "airingAt_lesser": now}
                })
                if r.status_code != 200:
                    break
                data = r.json().get("data", {}).get("Page", {})
                schedules = data.get("airingSchedules", [])
                if not schedules:
                    break

                valid_schedules = []
                batch_stmts = []
                for item in schedules:
                    m = item["media"]
                    anilist_id = m["id"]
                    if anilist_id in blacklisted:
                        continue

                    # Filter blocked formats or genres
                    format_type = m.get("format") or "TV"
                    if format_type not in ["TV", "TV_SHORT", "MOVIE", "SPECIAL", "OVA", "ONA"]:
                        continue

                    genres_list = m.get("genres") or []
                    if any(str(g).lower() in {"hentai", "ecchi"} for g in genres_list):
                        continue

                    valid_schedules.append(item)
                    romaji = m["title"]["romaji"] or ""
                    english = m["title"]["english"] or ""
                    native = m["title"]["native"] or ""
                    synonyms = json.dumps(m.get("synonyms") or [])
                    cover_url = m.get("coverImage", {}).get("large") or ""
                    banner_url = m.get("bannerImage") or ""
                    synopsis = m.get("description") or ""
                    genres = json.dumps(genres_list)

                    batch_stmts.append(("""
                        INSERT INTO anime (anilist_id, title_romaji, title_english, title_native, synonyms, 
                                           cover_url, banner_url, synopsis, genres, format, status)
                        VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, 'RELEASING')
                        ON CONFLICT(anilist_id) DO UPDATE SET
                            title_romaji = excluded.title_romaji,
                            title_english = excluded.title_english,
                            synonyms = excluded.synonyms,
                            cover_url = CASE WHEN anime.cover_url IS NULL OR anime.cover_url = '' THEN excluded.cover_url ELSE anime.cover_url END,
                            status = 'RELEASING'
                    """, [anilist_id, romaji, english, native, synonyms, cover_url, banner_url, synopsis, genres, format_type]))

                if batch_stmts:
                    await execute_sql_batch(batch_stmts)

                ep_batch = []
                for item in valid_schedules:
                    m = item["media"]
                    anilist_id = m["id"]
                    ep_num = item["episode"]
                    airing_ts = item["airingAt"]
                    ep_batch.append(("""
                        INSERT INTO episodes (anime_id, episode_number, status, aired_at)
                        VALUES ((SELECT id FROM anime WHERE anilist_id = ?), ?, 'pending', ?)
                        ON CONFLICT(anime_id, episode_number) DO NOTHING
                    """, [anilist_id, ep_num, airing_ts]))

                if ep_batch:
                    await execute_sql_batch(ep_batch)
                log_message(f"Page {page}: synced {len(valid_schedules)} entries.")

                has_next = data.get("pageInfo", {}).get("hasNextPage", False)
                page += 1
                await asyncio.sleep(1.5)
        except Exception as e:
            log_message(f"Schedule sync error on page {page}: {e}")
            break

# ─── Finished Anime Catch-up ───────────────────────────────────
async def check_finished_anime_catchup():
    """Checks actively-tracked anime that finished with missing final episodes on AniList."""
    tracked_anime = await execute_sql("""
        SELECT a.id, a.anilist_id, a.title_romaji, COUNT(e.id) as ready_count, MAX(CAST(e.episode_number AS INTEGER)) as max_ep
        FROM anime a
        JOIN episodes e ON a.id = e.anime_id
        WHERE e.status = 'ready' AND a.status = 'RELEASING'
        GROUP BY a.id
        HAVING ready_count >= 3
    """)

    if not tracked_anime:
        return

    anilist_ids = [r["anilist_id"] for r in tracked_anime if r.get("anilist_id")]
    aid_map = {r["anilist_id"]: r for r in tracked_anime}

    query = """
    query ($ids: [Int], $page: Int) {
      Page(page: $page, perPage: 50) {
        media(id_in: $ids, status: FINISHED) {
          id
          episodes
        }
        pageInfo { hasNextPage }
      }
    }
    """
    try:
        async with httpx.AsyncClient(timeout=20.0) as client:
            r = await client.post("https://graphql.anilist.co", json={"query": query, "variables": {"ids": anilist_ids, "page": 1}})
            if r.status_code == 200:
                finished = r.json().get("data", {}).get("Page", {}).get("media", [])
                now_ts = int(time.time())
                for item in finished:
                    aid_info = aid_map.get(item["id"])
                    if not aid_info:
                        continue
                    total_eps = item.get("episodes") or 0
                    max_ep = aid_info["max_ep"] or 0
                    if total_eps > max_ep:
                        for missing_ep in range(max_ep + 1, total_eps + 1):
                            await execute_sql("""
                                INSERT INTO episodes (anime_id, episode_number, status, aired_at)
                                VALUES (?, ?, 'pending', ?)
                                ON CONFLICT(anime_id, episode_number) DO NOTHING
                            """, [aid_info["id"], missing_ep, now_ts])
                        log_message(f"Catch-up: Queued missing episodes {max_ep+1}-{total_eps} for finished anime {aid_info['title_romaji']}")
    except Exception:
        pass

# ─── Main Episode Resolution Loop ─────────────────────────────
async def resolve_pending_episodes():
    log_message("Resolving pending episodes...")
    cutoff_ts = int(time.time()) - (14 * 24 * 60 * 60)
    pending_eps = await execute_sql("""
        SELECT e.id as ep_id, e.anime_id, e.episode_number, e.status, e.aired_at, e.last_checked,
               a.anilist_id, a.title_romaji, a.title_english, a.synonyms, a.format, a.erai_title
        FROM episodes e
        JOIN anime a ON e.anime_id = a.id
        WHERE e.status = 'pending'
          AND e.aired_at >= ?
          AND CAST(e.episode_number AS INTEGER) = (
              SELECT MIN(CAST(e2.episode_number AS INTEGER)) FROM episodes e2
              WHERE e2.anime_id = e.anime_id AND e2.status = 'pending' AND e2.aired_at >= ?
          )
        ORDER BY e.aired_at DESC, e.last_checked ASC
    """, [cutoff_ts, cutoff_ts])

    if not pending_eps:
        log_message("No pending episodes found.")
        return

    log_message(f"Found {len(pending_eps)} anime with pending episodes (lowest ep per anime).")
    downloads_count = 0

    for ep in pending_eps:
        if downloads_count >= MAX_DOWNLOADS_PER_RUN:
            log_message(f"Reached max downloads limit ({MAX_DOWNLOADS_PER_RUN}). Finishing cycle.")
            break

        ep_id = ep["ep_id"]
        anime_id = ep["anime_id"]
        anilist_id = ep.get("anilist_id")
        ep_num = ep["episode_number"]
        romaji = ep["title_romaji"]
        english = ep["title_english"]
        erai_title = ep.get("erai_title")
        synonyms = json.loads(ep["synonyms"]) if ep["synonyms"] else []
        is_special = ep["format"] in ["SPECIAL", "MOVIE", "OVA", "ONA"]

        now_ts = int(time.time())
        aired_at = ep.get("aired_at") or 0

        # Non-translated anime check: if Ep 1 aired > 7 days ago and was never found, blacklist.
        # Skip if the anime already has available episodes (e.g. ep2+ got subbed) so we never
        # wipe a working catalogue just because episode 1 lagged behind.
        if ep_num == 1 and aired_at > 0 and (now_ts - aired_at > 7 * 86400):
            existing = await execute_sql(
                "SELECT 1 FROM episodes WHERE anime_id = ? AND status = 'ready' LIMIT 1",
                [anime_id],
            )
            if existing:
                log_message(f"Ep1 grace expired but anime has available episodes; skipping blacklist: {romaji}")
            else:
                await blacklist_anime(anime_id, anilist_id, romaji, "first_episode_grace_expired")
            continue

        log_message(f"Searching torrents for: {romaji} (Ep {ep_num})")
        queries = get_search_queries(romaji, english, ep_num, synonyms=synonyms, is_special=is_special, erai_title=erai_title)
        
        all_results = []
        search_notes = []
        for i in range(0, min(len(queries), 6), 2):
            batch = queries[i:i+2]
            tasks = [
                search_nyaa_rss(q, romaji, english, ep_num, synonyms=synonyms, is_special=is_special)
                for q in batch
            ]
            batch_res = await asyncio.gather(*tasks, return_exceptions=True)
            for res in batch_res:
                if isinstance(res, Exception):
                    search_notes.append(f"task {type(res).__name__}")
                    continue
                res_list, res_note = res
                if res_note:
                    search_notes.append(res_note)
                if res_list:
                    all_results.extend(res_list)
            if any(r["seeders"] >= 50 and bool(re.search(r'\[?(erai[-_ ]?raws|toonshub)\]?', r["title"].lower())) for r in all_results):
                break
            if len(all_results) >= 10:
                break

        # Deduplicate
        seen_magnets = set()
        deduped = []
        for r in all_results:
            if r["magnet"] not in seen_magnets:
                seen_magnets.add(r["magnet"])
                deduped.append(r)

        tier3_only = (aired_at > 0) and (now_ts - aired_at < 600)

        def is_acceptable_torrent(t_title: str) -> bool:
            if tier3_only:
                return get_platform_score(t_title) >= 3
            return True

        def get_min_seeders_for_torrent(t_title: str) -> int:
            is_trusted = bool(re.search(r'\[?(erai[-_ ]?raws|toonshub)\]?', t_title.lower()))
            if is_trusted and (aired_at > 0) and (now_ts - aired_at < 7200):
                return 1
            elif is_trusted:
                return 2
            return MIN_TORRENT_SEEDERS

        # Date sanity check: If torrent was published on Nyaa > 7 days BEFORE AniList airing date, it is an outdated/false-positive match
        def is_valid_release_date(t_pub_date: int, ep_aired_at: int) -> bool:
            if not t_pub_date or not ep_aired_at or ep_aired_at <= 0:
                return True
            # Allow up to 7 days earlier in case of AniList slight schedule delay/early leaks
            if t_pub_date < (ep_aired_at - 7 * 86400):
                return False
            return True

        good = [
            r for r in deduped 
            if r["seeders"] >= get_min_seeders_for_torrent(r["title"]) 
            and is_acceptable_torrent(r["title"]) 
            and not is_blacklisted_platform(r["title"])
            and is_valid_release_date(r.get("pub_date", 0), aired_at)
        ]
        if not good:
            hint = ""
            if search_notes:
                unique_notes = list(dict.fromkeys(search_notes))
                hint = f" [{len(search_notes)} empty queries | {'; '.join(unique_notes[:3])}]"
            log_message(f"No active torrents found yet for {romaji} Ep {ep_num}.{hint}")
            await execute_sql("UPDATE episodes SET last_checked = ? WHERE id = ?", [int(time.time()), ep_id])
            continue

        # ── Arabic Subtitle Priority ───────────────────────────────
        # If multiple multi-subs from different platforms, check detail pages for Arabic
        def _has_arabic_variants(text: str) -> bool:
            if not text: return False
            t = text.lower()
            # All Arabic variants: ara, ar, arabic, العربية, عربي
            return bool(re.search(r'\barabic\b|\bara\b|(?<!\w)ar(?!\w)|العربية|عربي', t))

        # Identify multi-subs candidates from different platforms
        multi_subs_candidates = [r for r in good if bool(re.search(r'\b(multi|m)\s*[-_:]?\s*subs?\b|multisubs?', r["title"].lower()))]
        platforms_in_multi = set()
        for r in multi_subs_candidates:
            # Extract platform hint from title (cr, nf, etc.)
            if re.search(r'\b(cr|crunchyroll)\b', r["title"].lower()):
                platforms_in_multi.add('cr')
            elif re.search(r'\b(nf|netflix)\b', r["title"].lower()):
                platforms_in_multi.add('nf')
            elif re.search(r'\b(amzn|amazon)\b', r["title"].lower()):
                platforms_in_multi.add('amzn')
            elif re.search(r'\b(bilibili|bili)\b', r["title"].lower()):
                platforms_in_multi.add('bili')
            else:
                platforms_in_multi.add('other')

        # Only do extra fetch if at least 2 different platforms with multi-subs
        arabic_cache = {}
        if len(multi_subs_candidates) >= 2 and len(platforms_in_multi) >= 2:
            async def _check_arabic_for_item(item):
                magnet = item.get("magnet", "")
                # Derive view URL from download URL: /download/ -> /view/
                view_url = None
                if "nyaa.si/download/" in magnet:
                    view_url = magnet.replace("/download/", "/view/").split(".torrent")[0]
                elif "nyaa.si/view/" in magnet:
                    view_url = magnet
                else:
                    # Try to use magnet as is (may be view page via proxy)
                    view_url = magnet
                # Try GAS proxy for detail page or direct fetch
                for proxy_base in get_ordered_proxies():
                    try:
                        # Nyaa view page is HTML, not JSON - try direct via proxy with view url
                        async with httpx.AsyncClient(timeout=15.0, follow_redirects=True) as client:
                            # Use proxy to fetch view page HTML via ?url= param if proxy supports it, else try direct
                            # For now, try to fetch via proxy's torrent mode with view url
                            gas_url = f"{proxy_base}?mode=torrent&url={urllib.parse.quote(view_url)}"
                            # Alternative: try to fetch HTML directly via proxy's q param if it's a search proxy
                            # Fallback to direct fetch of view_url
                            r = await client.get(view_url, headers={"User-Agent": "Mozilla/5.0"})
                            if r.status_code == 200 and "Subtitles Info" in r.text:
                                return _has_arabic_variants(r.text)
                            # Try via GAS proxy as search
                            r2 = await client.get(gas_url)
                            if r2.status_code == 200:
                                try:
                                    data = r2.json()
                                    if data.get("data"):
                                        import base64
                                        html = base64.b64decode(data["data"]).decode('utf-8', errors='ignore')
                                        return _has_arabic_variants(html)
                                except: pass
                                return _has_arabic_variants(r2.text)
                    except: continue
                # If all fails, check title itself for Arabic hint (fallback)
                return _has_arabic_variants(item.get("title",""))

            # Run checks concurrently for all multi-subs candidates
            check_tasks = [ _check_arabic_for_item(r) for r in good ]
            try:
                results = await asyncio.gather(*check_tasks, return_exceptions=True)
                for idx, res in enumerate(results):
                    if isinstance(res, Exception):
                        arabic_cache[good[idx]["magnet"]] = False
                    else:
                        arabic_cache[good[idx]["magnet"]] = bool(res)
                        if res:
                            log_message(f"Arabic subtitle found in: {good[idx]['title'][:60]}")
            except Exception as e:
                log_message(f"Arabic check failed: {e}")

        # Smart Sort: Arabic (if checked) > Multi-Audio > Trusted Groups > Platform Score > Quality > Source > Seeders
        def _arabic_score(item):
            return 1 if arabic_cache.get(item["magnet"], False) else 0

        # Only apply Arabic priority if we actually checked (cache not empty)
        if arabic_cache:
            good.sort(key=lambda x: (
                _arabic_score(x),
                get_audio_score(x["title"]),
                1 if ("[erai-raws]" in x["title"].lower() or "[toonshub]" in x["title"].lower()) else 0,
                get_platform_score(x["title"]),
                get_quality_weight(x["title"]),
                get_source_weight(x["title"]),
                x["seeders"]
            ), reverse=True)
        else:
            good.sort(key=lambda x: (
                get_audio_score(x["title"]),
                1 if ("[erai-raws]" in x["title"].lower() or "[toonshub]" in x["title"].lower()) else 0,
                get_platform_score(x["title"]),
                get_quality_weight(x["title"]),
                get_source_weight(x["title"]),
                x["seeders"]
            ), reverse=True)

        winner = good[0]
        torrent_title = winner["title"]
        audio_score = get_audio_score(torrent_title)
        is_multi_audio = 1 if audio_score >= 1 else 0

        log_message(f"Selected: {torrent_title} (Seeders: {winner['seeders']}, Audio Score: {audio_score})")

        dl_dir = None
        try:
            dl_dir, v_path, v_name, v_size, info_hash = await asyncio.to_thread(download_torrent, winner["magnet"], torrent_title)
            size_mb = round(v_size / 1048576, 2)
            stored_source = (
                f"magnet:?xt=urn:btih:{info_hash}&dn={urllib.parse.quote(torrent_title)}"
                if info_hash else winner["magnet"]
            )

            upload = await asyncio.to_thread(upload_pixeldrain, v_path, v_name)
            pd_id = upload["id"]
            pd_url = upload["url"]

            now_str = datetime.datetime.now().strftime("%Y-%m-%d %H:%M:%S")
            # Grace period: if not CR, keep as ready but still check for CR with Arabic for 1 hour (user sees episode immediately)
            is_cr = get_platform_score(torrent_title) >= 3
            if not is_cr:
                pending_until = int(time.time()) + 3600
                await execute_sql("""
                    UPDATE episodes 
                    SET status = 'ready',
                        stream_url = ?,
                        pixeldrain_id = ?,
                        pixeldrain_1080_url = ?,
                        pixeldrain_1080_id = ?,
                        file_size_mb = ?,
                        magnet_link = ?,
                        is_multi_audio = ?,
                        audio_score = ?,
                        uploaded_at = ?,
                        last_checked = ?,
                        pending_review_until = ?
                    WHERE id = ?
                """, [pd_url, pd_id, pd_url, pd_id, size_mb, stored_source, is_multi_audio, audio_score, now_str, int(time.time()), pending_until, ep_id])
                log_message(f"Non-CR torrent for {romaji} Ep {ep_num} - visible as ready, grace period 1h until {pending_until} to wait for CR with Arabic")
            else:
                await execute_sql("""
                    UPDATE episodes 
                    SET status = 'ready',
                        stream_url = ?,
                        pixeldrain_id = ?,
                        pixeldrain_1080_url = ?,
                        pixeldrain_1080_id = ?,
                        file_size_mb = ?,
                        magnet_link = ?,
                        is_multi_audio = ?,
                        audio_score = ?,
                        uploaded_at = ?,
                        last_checked = ?,
                        pending_review_until = 0
                    WHERE id = ?
                """, [pd_url, pd_id, pd_url, pd_id, size_mb, stored_source, is_multi_audio, audio_score, now_str, int(time.time()), ep_id])

            # Store parsed erai_title for future searches
            parsed_erai = parse_erai_anime_title(v_name)
            if parsed_erai and not erai_title:
                await execute_sql("UPDATE anime SET erai_title = ? WHERE id = ?", [parsed_erai, anime_id])

            log_message(f"Successfully processed {romaji} Ep {ep_num}")
            downloads_count += 1

        except Exception as ex:
            log_message(f"Failed to process {romaji} Ep {ep_num}: {ex}")
            await execute_sql("UPDATE episodes SET last_checked = ? WHERE id = ?", [int(time.time()), ep_id])
        finally:
            if dl_dir:
                shutil.rmtree(dl_dir, ignore_errors=True)

# ─── Pending Review Grace Period (Non-CR -> wait for CR with Arabic) ──
async def check_pending_reviews():
    log_message("Checking pending reviews (non-CR grace period)...")
    pending = await execute_sql("""
        SELECT e.id as ep_id, e.anime_id, e.episode_number, e.pixeldrain_id, e.pixeldrain_1080_url,
               e.pending_review_until, a.title_romaji, a.title_english, a.synonyms, a.format, a.erai_title
        FROM episodes e
        JOIN anime a ON e.anime_id = a.id
        WHERE e.status = 'ready' AND e.pending_review_until > 0
        ORDER BY e.pending_review_until ASC
        LIMIT 10
    """)
    if not pending:
        return
    now = int(time.time())
    for ep in pending:
        ep_id = ep["ep_id"]
        until = ep.get("pending_review_until") or 0
        romaji = ep["title_romaji"]
        english = ep["title_english"]
        ep_num = ep["episode_number"]
        # If grace period expired, clear pending flag (keep as ready)
        if now >= until:
            await execute_sql("UPDATE episodes SET pending_review_until = 0 WHERE id = ?", [ep_id])
            log_message(f"Grace expired for {romaji} Ep {ep_num} -> keep ready, no CR found")
            continue
        # Still within grace period, search for CR with Arabic
        synonyms = json.loads(ep["synonyms"]) if ep["synonyms"] else []
        erai_title = ep.get("erai_title")
        is_special = ep.get("format") in ["SPECIAL", "MOVIE", "OVA", "ONA"]
        queries = get_search_queries(romaji, english, ep_num, synonyms=synonyms, is_special=is_special, erai_title=erai_title)
        found_better = None
        for i in range(0, min(len(queries), 4), 2):
            batch = queries[i:i+2]
            tasks = [search_nyaa_rss(q, romaji, english, ep_num, synonyms=synonyms, is_special=is_special) for q in batch]
            batch_res = await asyncio.gather(*tasks, return_exceptions=True)
            for res in batch_res:
                if isinstance(res, Exception):
                    continue
                res_list, _ = res
                for r in res_list:
                    # Only consider CR
                    if get_platform_score(r["title"]) < 3:
                        continue
                    if not bool(re.search(r'\b(multi|m)\s*[-_:]?\s*subs?\b|multisubs?', r["title"].lower())):
                        continue
                    # Check for Arabic in detail page (reuse cache from earlier Arabic check)
                    # For pending review, we do a quick title check for Arabic as proxy
                    if re.search(r'\barabic\b|\bara\b|العربية|عربي', r["title"].lower()):
                        found_better = r
                        break
                if found_better:
                    break
            if found_better:
                break
        if found_better:
            log_message(f"Grace: CR with Arabic found for {romaji} Ep {ep_num}: {found_better['title']}")
            dl_dir = None
            try:
                dl_dir, v_path, v_name, v_size, info_hash = await asyncio.to_thread(download_torrent, found_better["magnet"], found_better["title"])
                size_mb = round(v_size / 1048576, 2)
                stored_source = f"magnet:?xt=urn:btih:{info_hash}&dn={urllib.parse.quote(found_better['title'])}" if info_hash else found_better["magnet"]
                upload = await asyncio.to_thread(upload_pixeldrain, v_path, v_name)
                if ep["pixeldrain_id"]:
                    delete_from_pixeldrain(ep["pixeldrain_id"])
                pd_id = upload["id"]
                pd_url = upload["url"]
                now_str = datetime.datetime.now().strftime("%Y-%m-%d %H:%M:%S")
                await execute_sql("""
                    UPDATE episodes SET status = 'ready', stream_url = ?, pixeldrain_id = ?, pixeldrain_1080_url = ?, pixeldrain_1080_id = ?,
                        file_size_mb = ?, magnet_link = ?, uploaded_at = ?, pending_review_until = 0 WHERE id = ?
                """, [pd_url, pd_id, pd_url, pd_id, size_mb, stored_source, now_str, ep_id])
                log_message(f"Grace: replaced {romaji} Ep {ep_num} with CR Arabic version")
            except Exception as e:
                log_message(f"Grace: failed to replace {romaji} Ep {ep_num}: {e}")
            finally:
                if dl_dir:
                    shutil.rmtree(dl_dir, ignore_errors=True)

# ─── Audio Upgrade Monitor ─────────────────────────────────────
async def check_audio_upgrades():
    log_message("Checking for quality upgrades...")
    recent_eps = await execute_sql("""
        SELECT e.id as ep_id, e.anime_id, e.episode_number, e.pixeldrain_id, e.audio_score, e.uploaded_at,
               a.title_romaji, a.title_english, a.synonyms, a.format, a.erai_title
        FROM episodes e
        JOIN anime a ON e.anime_id = a.id
        WHERE e.status = 'ready' AND (e.audio_score < 2 OR e.audio_score IS NULL)
        ORDER BY e.id DESC
        LIMIT 15
    """)

    for ep in recent_eps:
        current_audio = ep["audio_score"] or 0
        romaji = ep["title_romaji"]
        english = ep["title_english"]
        erai_title = ep.get("erai_title")
        ep_num = ep["episode_number"]
        synonyms = json.loads(ep["synonyms"]) if ep["synonyms"] else []

        is_special = ep.get("format") in ["SPECIAL", "MOVIE", "OVA", "ONA"]
        queries = get_search_queries(romaji, english, ep_num, synonyms=synonyms, is_special=is_special, erai_title=erai_title)
        better = []
        for i in range(0, min(len(queries), 4), 2):
            batch = queries[i:i+2]
            tasks = [search_nyaa_rss(q, romaji, english, ep_num, synonyms=synonyms, is_special=is_special) for q in batch]
            batch_res = await asyncio.gather(*tasks, return_exceptions=True)
            for res in batch_res:
                if isinstance(res, Exception):
                    continue
                res_list, _ = res
                if res_list:
                    for r in res_list:
                        if get_audio_score(r["title"]) > current_audio and r.get("seeders", 0) >= 1:
                            better.append(r)
            if better:
                break

        if better:
            better.sort(key=lambda x: (get_audio_score(x["title"]), x["seeders"]), reverse=True)
            target = better[0]
            new_score = get_audio_score(target["title"])
            log_message(f"Audio upgrade found for {romaji} Ep {ep_num}! Upgrading score {current_audio} -> {new_score} using: {target['title']}")
            
            dl_dir = None
            try:
                dl_dir, v_path, v_name, v_size, info_hash = await asyncio.to_thread(download_torrent, target["magnet"], target["title"])
                size_mb = round(v_size / 1048576, 2)
                stored_source = (
                    f"magnet:?xt=urn:btih:{info_hash}&dn={urllib.parse.quote(target['title'])}"
                    if info_hash else target["magnet"]
                )
                upload = await asyncio.to_thread(upload_pixeldrain, v_path, v_name)

                if ep["pixeldrain_id"]:
                    delete_from_pixeldrain(ep["pixeldrain_id"])

                pd_id = upload["id"]
                pd_url = upload["url"]
                await execute_sql("""
                    UPDATE episodes
                    SET stream_url = ?, pixeldrain_id = ?, pixeldrain_1080_url = ?, pixeldrain_1080_id = ?,
                        file_size_mb = ?, magnet_link = ?, is_multi_audio = 1, audio_score = ?
                    WHERE id = ?
                """, [pd_url, pd_id, pd_url, pd_id, size_mb, stored_source, new_score, ep["ep_id"]])

                log_message(f"Successfully upgraded {romaji} Ep {ep_num} audio.")
            except Exception as up_ex:
                log_message(f"Failed to apply audio upgrade for {romaji}: {up_ex}")
            finally:
                if dl_dir:
                    shutil.rmtree(dl_dir, ignore_errors=True)

# ─── Main Entry Point ──────────────────────────────────────────
async def main():
    log_message("=== Starting Data Sync Pipeline ===")
    t0 = time.time()
    await ensure_database_schema()
    await cleanup_pixeldrain_duplicates()
    await sync_anilist_schedule()
    await check_finished_anime_catchup()
    await resolve_pending_episodes()
    await check_pending_reviews()
    await check_audio_upgrades()
    elapsed = round(time.time() - t0, 1)
    log_message(f"=== Job Finished in {elapsed}s ===")

if __name__ == "__main__":
    asyncio.run(main())
