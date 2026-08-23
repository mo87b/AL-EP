import os
import sys
import re
import json
import time
import shutil
import tempfile
import asyncio
import datetime
import subprocess
import urllib.parse
import xml.etree.ElementTree as ET
import httpx

# ─── Environment Configuration ──────────────────────────────────
TURSO_URL = os.environ.get("TURSO_URL", "")
TURSO_TOKEN = os.environ.get("TURSO_TOKEN", "")
PIXELDRAIN_API_KEY = os.environ.get("PIXELDRAIN_API_KEY", "")
GAS_PROXY_URL = os.environ.get("GAS_PROXY_URL", "")

MAX_DOWNLOADS_PER_RUN = 5
TORRENT_DOWNLOAD_TIMEOUT = 300  # 5 minutes per download
MIN_TORRENT_SEEDERS = 7

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
    async with httpx.AsyncClient(timeout=25.0) as client:
        r = await client.post(f"{TURSO_URL}/v2/pipeline", headers=headers, json=body)
        if r.status_code != 200:
            log_message(f"DB error ({r.status_code})")
            return []
        res = r.json()
        exec_result = res["results"][0]["response"]["result"]
        return _parse_turso_result(exec_result)

async def execute_sql_batch(statements: list) -> list:
    """Execute multiple SQL statements in a single HTTP request.
    statements: list of (sql, args) tuples.
    Returns list of results for each statement."""
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
            # skip "close" responses
        return results

# ─── Title & Episode Parsing Functions ─────────────────────────
def clean_title(title: str) -> str:
    if not title or not isinstance(title, str):
        return ""
    title = re.sub(r'\(.*?\)', '', title)
    title = re.sub(r'\[.*?\]', '', title)
    title = re.sub(r'[:\\/*?"<>|]', ' ', title)
    title = re.sub(r'[^a-zA-Z0-9\s\-\'\.]', '', title)
    title = re.sub(r'\s+', ' ', title).strip()
    return title

def clean_and_strip(title: str) -> str:
    t = clean_title(title)
    t = re.sub(r'\b\d{4}\b', ' ', t)
    t = re.sub(r'\b\d+(st|nd|rd|th)\s+season\b', '', t, flags=re.IGNORECASE)
    t = re.sub(r'\bseason\s+\d+\b', '', t, flags=re.IGNORECASE)
    t = re.sub(r'\bcour\s+\d+\b', '', t, flags=re.IGNORECASE)
    t = re.sub(r'\bs\d+\b', '', t, flags=re.IGNORECASE)
    return re.sub(r'\s+', ' ', t).strip()

def get_part_number(title: str) -> int:
    if not title or not isinstance(title, str):
        return 0
    t_lower = title.lower()
    
    # Check Roman numerals (e.g. Part II, Cour III)
    m = re.search(r'\b(?:part|cour|pt)\s*[-_.: ]*\s*(iv|iii|ii|i)\b', t_lower)
    if m:
        roman_map = {'i': 1, 'ii': 2, 'iii': 3, 'iv': 4}
        return roman_map.get(m.group(1), 0)
    
    # Check Ordinal variations (e.g. 2nd Part, 2nd Cour)
    m = re.search(r'\b(\d+)(st|nd|rd|th)\s+(?:part|cour|pt)\b', t_lower)
    if m:
        return int(m.group(1))

    # Check Numeric variations (e.g. Part 2, Part-2, Cour.2)
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

    if re.search(r'\bii\b$', title_lower) or re.search(r'\bii\b(?=\s)', title_lower):
        return 2
    if re.search(r'\biii\b$', title_lower) or re.search(r'\biii\b(?=\s)', title_lower):
        return 3
    if re.search(r'\biv\b$', title_lower) or re.search(r'\biv\b(?=\s)', title_lower):
        return 4
    if re.search(r'\bv\b$', title_lower) or re.search(r'\bv\b(?=\s)', title_lower):
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

# Comprehensive words to ignore during title match to avoid failing on season tokens or release formats
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
    clean_t = title_no_num.replace('.', ' ').replace('-', ' ')
    clean_t = clean_t.replace("'", "")
    clean_t = re.sub(r'[^a-zA-Z0-9\s]', ' ', clean_t)
    words = clean_t.split()
    
    particles = {
        "no", "to", "in", "of", "a", "an", "the", "is", "at", "by", "on", 
        "and", "or", "for", "with", "wa", "ga", "wo", "ni", "de", "ka", "mo"
    }
    
    filtered = []
    for w in words:
        w_stripped = w.strip("-'")
        if not w_stripped:
            continue
        if w_stripped in SEASON_STOPWORDS or w_stripped in particles:
            continue
        if len(w_stripped) >= 2:
            filtered.append(w_stripped)
        elif len(words) == 1:
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

    # 3. Title Matching with ratio & delimiter support
    def is_title_match(anime_title: str, torrent_title_lower: str) -> bool:
        if not anime_title:
            return False
            
        def check_match(raw_title_str: str) -> bool:
            clean_t = clean_title(raw_title_str)
            words = get_clean_words(clean_t)
            if not words:
                return False
            matching_words = [w for w in words if w in torrent_title_lower]
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
    syn_match = False
    for syn in synonyms:
        if syn and is_title_match(syn, t_lower):
            syn_match = True
            break

    if not romaji_match and not eng_match and not syn_match:
        return False

    # 4. Extra words check (with Japanese concatenation check)
    is_trusted_group = bool(re.search(r'\[?(erai[-_ ]?raws|toonshub)\]?', t_lower))
    if not is_trusted_group:
        torrent_clean = clean_title(torrent_title)
        torrent_words = get_clean_words(torrent_clean)
        
        anime_words = set()
        anime_words.update(get_clean_words(romaji))
        if english:
            anime_words.update(get_clean_words(english))
        for syn in synonyms:
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
                if len(w1) >= 3 and w.startswith(w1):
                    remainder = w[len(w1):]
                    if remainder in anime_words:
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
    if not is_multi_sub:
        return False

    return True

def get_search_queries(romaji: str, english: str, ep: int, synonyms: list = None, is_special: bool = False) -> list:
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
    
    search_bases = [r_base]
    if r_super and r_super not in search_bases:
        search_bases.append(r_super)
    if e_base and e_base not in search_bases:
        search_bases.append(e_base)
    if e_super and e_super not in search_bases:
        search_bases.append(e_super)
        
    for syn in synonyms:
        cleaned_syn = clean_and_strip(syn)
        if cleaned_syn and cleaned_syn not in search_bases:
            search_bases.append(cleaned_syn)
        syn_super = re.sub(r'[\-\.]', ' ', cleaned_syn).replace("'", "")
        syn_super = re.sub(r'\s+', ' ', syn_super).strip()
        if syn_super and syn_super not in search_bases:
            search_bases.append(syn_super)
            
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
                if is_special and ep == 1:
                    queries.append(var)
                words = var.split()
                if len(words) > 3:
                    short_var = " ".join(words[:3])
                    queries.append(f'{short_var} "{ep_str}"')
                    queries.append(f'{short_var} {ep_str}')
                    if is_special and ep == 1:
                        queries.append(short_var)
                    
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

# ─── Nyaa Search & Proxy Integration ──────────────────────────
async def search_nyaa_rss(query: str, romaji: str, english: str, ep: int, synonyms: list = None, is_special: bool = False) -> list:
    encoded_query = urllib.parse.quote(query)
    sources = [
        {"type": "rss", "url": f"{GAS_PROXY_URL}?q={encoded_query}"},
        {"type": "rss", "url": f"https://nyaa.si/?page=rss&q={encoded_query}"},
        {"type": "rss", "url": f"https://nyaa.site/?page=rss&q={encoded_query}"}
    ]

    headers = {"User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36"}

    for src in sources:
        try:
            await asyncio.sleep(0.3)
            async with httpx.AsyncClient(timeout=25.0, headers=headers, follow_redirects=True) as client:
                r = await client.get(src["url"])
                if r.status_code == 200:
                    raw_items = []
                    if src.get("type") == "api" or (r.text.startswith("{") and "data" in r.text):
                        data = r.json()
                        items = data.get("data", [])
                        for item in items:
                            raw_items.append({
                                "title": item.get("title", ""),
                                "torrent": item.get("torrent", ""),
                                "seeders": int(item.get("seeders") or 0)
                            })
                    elif "<rss" in r.text or "<item" in r.text:
                        root = ET.fromstring(r.content)
                        items = root.findall(".//item")
                        for item in items:
                            title_el = item.find("title")
                            link_el = item.find("link")
                            title = title_el.text if title_el is not None else ""
                            torrent_url = link_el.text if link_el is not None else ""
                            seeders = 0
                            for child in item:
                                if child.tag.endswith("seeders"):
                                    seeders = int(child.text or 0) if child.text and child.text.isdigit() else 0
                                    break
                            raw_items.append({
                                "title": title,
                                "torrent": torrent_url,
                                "seeders": seeders
                            })

                    results = []
                    for item in raw_items:
                        t = item["title"]
                        torrent_url = item["torrent"]
                        seeders = item["seeders"]
                        if not t or not torrent_url:
                            continue

                        if is_matching_torrent(t, romaji, english, ep, synonyms=synonyms, is_special=is_special):
                            id_match = re.search(r'/(?:download|view)/(\d+)', torrent_url)
                            if id_match:
                                torrent_id = id_match.group(1)
                            else:
                                torrent_id = torrent_url.split('/')[-1].split('.')[0]

                            torrent_source = f"https://nyaa.iss.one/download/{torrent_id}.torrent"
                            results.append({
                                "title": t,
                                "magnet": torrent_source,
                                "seeders": seeders
                            })
                    if results:
                        return results
        except Exception:
            pass

    return []

# ─── aria2c Downloader & Pixeldrain Uploader ──────────────────
def download_torrent(magnet_url: str, torrent_title: str) -> tuple:
    download_dir = tempfile.mkdtemp(prefix="anime_")
    torrent_file_path = os.path.join(download_dir, "download.torrent")
    
    # If source is HTTP .torrent URL, download through GAS proxy
    if magnet_url.startswith("http"):
        gas_url = f"{GAS_PROXY_URL}?mode=torrent&url={urllib.parse.quote(magnet_url)}"
        try:
            with httpx.Client(timeout=30.0) as client:
                r = client.get(gas_url)
                if r.status_code == 200 and r.json().get("status") == 200:
                    import base64
                    raw_bytes = base64.b64decode(r.json()["data"])
                    with open(torrent_file_path, "wb") as f:
                        f.write(raw_bytes)
                    torrent_input = torrent_file_path
                else:
                    torrent_input = magnet_url
        except Exception:
            torrent_input = magnet_url
    else:
        torrent_input = magnet_url

    trackers_arg = ",".join(NYAA_TRACKERS)
    cmd = [
        "aria2c", torrent_input,
        f"--dir={download_dir}",
        "--seed-time=0",
        "--bt-stop-timeout=120",
        "--file-allocation=none",
        f"--bt-tracker={trackers_arg}",
        "--max-connection-per-server=16",
        "--summary-interval=10",
        "--allow-overwrite=true",
    ]

    log_message(f"Starting download...")
    proc = subprocess.run(cmd, timeout=TORRENT_DOWNLOAD_TIMEOUT, capture_output=True, text=True)
    if proc.returncode != 0:
        shutil.rmtree(download_dir, ignore_errors=True)
        raise RuntimeError(f"aria2c failed with code {proc.returncode}")

    # Find largest video file in download directory
    video_files = []
    for root, _, files in os.walk(download_dir):
        for f in files:
            if f.endswith((".mkv", ".mp4", ".avi")):
                fp = os.path.join(root, f)
                video_files.append((fp, f, os.path.getsize(fp)))

    if not video_files:
        shutil.rmtree(download_dir, ignore_errors=True)
        raise RuntimeError("No video file found in downloaded torrent!")

    video_files.sort(key=lambda x: x[2], reverse=True)
    best_file = video_files[0]
    return download_dir, best_file[0], best_file[1], best_file[2]

def upload_pixeldrain(file_path: str, filename: str) -> dict:
    url = f"https://pixeldrain.com/api/file/{urllib.parse.quote(filename)}"
    headers = {}
    auth = ("", PIXELDRAIN_API_KEY) if PIXELDRAIN_API_KEY else None

    log_message(f"Uploading {filename}...")
    with open(file_path, "rb") as f:
        with httpx.Client(timeout=300.0) as client:
            r = client.put(url, content=f.read(), auth=auth, headers=headers)
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
      }
    }
  }
}
"""

async def sync_anilist_schedule():
    log_message("Syncing schedule...")
    now = int(time.time())
    twelve_days_ago = now - (12 * 24 * 60 * 60)
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

                # Batch all upserts for this page into a single Turso HTTP call
                batch_stmts = []
                for item in schedules:
                    ep_num = item["episode"]
                    airing_ts = item["airingAt"]
                    m = item["media"]
                    anilist_id = m["id"]
                    romaji = m["title"]["romaji"] or ""
                    english = m["title"]["english"] or ""
                    native = m["title"]["native"] or ""
                    synonyms = json.dumps(m.get("synonyms") or [])
                    cover_url = m.get("coverImage", {}).get("large") or ""
                    banner_url = m.get("bannerImage") or ""
                    synopsis = m.get("description") or ""
                    genres = json.dumps(m.get("genres") or [])
                    format_type = m.get("format") or "TV"

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

                # Execute all anime upserts in one batch
                await execute_sql_batch(batch_stmts)

                # Now batch episode inserts (need anime IDs first)
                ep_batch = []
                for item in schedules:
                    m = item["media"]
                    anilist_id = m["id"]
                    ep_num = item["episode"]
                    airing_ts = item["airingAt"]
                    ep_batch.append(("""
                        INSERT INTO episodes (anime_id, episode_number, status, aired_at)
                        VALUES ((SELECT id FROM anime WHERE anilist_id = ?), ?, 'pending', ?)
                        ON CONFLICT(anime_id, episode_number) DO NOTHING
                    """, [anilist_id, ep_num, airing_ts]))

                await execute_sql_batch(ep_batch)
                log_message(f"Page {page}: synced {len(schedules)} entries.")

                has_next = data.get("pageInfo", {}).get("hasNextPage", False)
                page += 1
                await asyncio.sleep(1.5)
        except Exception as e:
            log_message(f"Schedule sync error on page {page}: {e}")
            break

# ─── Main Episode Resolution Loop ─────────────────────────────
async def resolve_pending_episodes():
    log_message("Resolving pending episodes...")
    # Only get the LOWEST pending episode per anime (no point searching Ep 337 if Ep 1-336 aren't done)
    # Also only consider episodes aired in the last 14 days to avoid searching for ancient backlog
    cutoff_ts = int(time.time()) - (14 * 24 * 60 * 60)
    pending_eps = await execute_sql("""
        SELECT e.id as ep_id, e.anime_id, e.episode_number, e.status, e.aired_at, e.last_checked,
               a.title_romaji, a.title_english, a.synonyms, a.format
        FROM episodes e
        JOIN anime a ON e.anime_id = a.id
        WHERE e.status = 'pending'
          AND e.aired_at >= ?
          AND e.episode_number = (
              SELECT MIN(e2.episode_number) FROM episodes e2
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
        ep_num = ep["episode_number"]
        romaji = ep["title_romaji"]
        english = ep["title_english"]
        synonyms = json.loads(ep["synonyms"]) if ep["synonyms"] else []
        is_special = ep["format"] in ["SPECIAL", "MOVIE", "OVA", "ONA"]

        log_message(f"Searching torrents for: {romaji} (Ep {ep_num})")
        queries = get_search_queries(romaji, english, ep_num, synonyms=synonyms, is_special=is_special)
        
        all_results = []
        for i, q in enumerate(queries):
            res_list = await search_nyaa_rss(q, romaji, english, ep_num, synonyms=synonyms, is_special=is_special)
            if res_list:
                all_results.extend(res_list)
                if any(r["seeders"] >= 50 and bool(re.search(r'\[?(erai[-_ ]?raws|toonshub)\]?', r["title"].lower())) for r in res_list):
                    break
                if len(all_results) >= 10 or i >= 3:
                    break

        # Deduplicate
        seen_magnets = set()
        deduped = []
        for r in all_results:
            if r["magnet"] not in seen_magnets:
                seen_magnets.add(r["magnet"])
                deduped.append(r)

        now_ts = int(time.time())
        aired_at = ep.get("aired_at") or 0
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

        good = [
            r for r in deduped 
            if r["seeders"] >= get_min_seeders_for_torrent(r["title"]) 
            and is_acceptable_torrent(r["title"]) 
            and not is_blacklisted_platform(r["title"])
        ]
        if not good:
            log_message(f"No active torrents found yet for {romaji} Ep {ep_num}.")
            await execute_sql("UPDATE episodes SET last_checked = ? WHERE id = ?", [int(time.time()), ep_id])
            continue

        # Smart Sort
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

        log_message(f"Selected: {romaji} Ep {ep_num} (Seeders: {winner['seeders']}, Audio: {audio_score})")

        try:
            dl_dir, v_path, v_name, v_size = await asyncio.to_thread(download_torrent, winner["magnet"], torrent_title)
            size_mb = round(v_size / 1048576, 2)

            upload = await asyncio.to_thread(upload_pixeldrain, v_path, v_name)
            shutil.rmtree(dl_dir, ignore_errors=True)

            pd_id = upload["id"]
            pd_url = upload["url"]

            now_str = datetime.datetime.now().strftime("%Y-%m-%d %H:%M:%S")
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
                    last_checked = ?
                WHERE id = ?
            """, [pd_url, pd_id, pd_url, pd_id, size_mb, winner["magnet"], is_multi_audio, audio_score, now_str, int(time.time()), ep_id])

            log_message(f"Successfully processed {romaji} Ep {ep_num}")
            downloads_count += 1

        except Exception as ex:
            log_message(f"Failed to process {romaji} Ep {ep_num}: {ex}")
            await execute_sql("UPDATE episodes SET last_checked = ? WHERE id = ?", [int(time.time()), ep_id])

# ─── Audio Upgrade Monitor ─────────────────────────────────────
async def check_audio_upgrades():
    log_message("Checking for quality upgrades...")
    recent_eps = await execute_sql("""
        SELECT e.id as ep_id, e.anime_id, e.episode_number, e.pixeldrain_id, e.audio_score, e.uploaded_at,
               a.title_romaji, a.title_english, a.synonyms, a.format
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
        ep_num = ep["episode_number"]
        synonyms = json.loads(ep["synonyms"]) if ep["synonyms"] else []

        queries = get_search_queries(romaji, english, ep_num, synonyms=synonyms)
        for q in queries[:4]:
            results = await search_nyaa_rss(q, romaji, english, ep_num, synonyms=synonyms)
            better = [r for r in results if get_audio_score(r["title"]) > current_audio and r["seeders"] >= 1]
            if better:
                better.sort(key=lambda x: (get_audio_score(x["title"]), x["seeders"]), reverse=True)
                target = better[0]
                new_score = get_audio_score(target["title"])
                log_message(f"Audio upgrade found for {romaji} Ep {ep_num}! Upgrading score {current_audio} -> {new_score}")
                
                try:
                    dl_dir, v_path, v_name, v_size = await asyncio.to_thread(download_torrent, target["magnet"], target["title"])
                    size_mb = round(v_size / 1048576, 2)
                    upload = await asyncio.to_thread(upload_pixeldrain, v_path, v_name)
                    shutil.rmtree(dl_dir, ignore_errors=True)

                    # Delete old file
                    if ep["pixeldrain_id"]:
                        delete_from_pixeldrain(ep["pixeldrain_id"])

                    pd_id = upload["id"]
                    pd_url = upload["url"]
                    await execute_sql("""
                        UPDATE episodes
                        SET stream_url = ?, pixeldrain_id = ?, pixeldrain_1080_url = ?, pixeldrain_1080_id = ?,
                            file_size_mb = ?, magnet_link = ?, is_multi_audio = 1, audio_score = ?
                        WHERE id = ?
                    """, [pd_url, pd_id, pd_url, pd_id, size_mb, target["magnet"], new_score, ep["ep_id"]])

                    log_message(f"Successfully upgraded {romaji} Ep {ep_num} audio.")
                    break
                except Exception as up_ex:
                    log_message(f"Failed to apply audio upgrade for {romaji}: {up_ex}")

# ─── Main Entry Point ──────────────────────────────────────────
async def main():
    log_message("=== Starting Data Sync Pipeline ===")
    t0 = time.time()
    await sync_anilist_schedule()
    await resolve_pending_episodes()
    await check_audio_upgrades()
    elapsed = round(time.time() - t0, 1)
    log_message(f"=== Job Finished in {elapsed}s ===")

if __name__ == "__main__":
    asyncio.run(main())
