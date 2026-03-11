import feedparser
import requests
from bs4 import BeautifulSoup
import json
import re
import os
import time
import hashlib
from collections import Counter, OrderedDict
from datetime import datetime, timedelta, timezone
from concurrent.futures import ThreadPoolExecutor, as_completed
from openai import OpenAI
from pydantic import BaseModel, Field
from typing import List, Literal, Optional
from email.utils import parsedate_to_datetime
from calendar import timegm

# Configuration
OPENAI_API_KEY = os.environ.get("OPENAI_API_KEY", "")
OPENAI_API_KEY2 = os.environ.get("OPENAI_API_KEY2", "")
if not OPENAI_API_KEY:
    raise RuntimeError("OPENAI_API_KEY environment variable is required")

VYZYVATEL_API_KEY = os.environ.get("VYZYVATEL_API_KEY", "")
VYZYVATEL_SET_ID = os.environ.get("VYZYVATEL_SET_ID", "5402")
DISCORD_WEBHOOK_URL = os.environ.get("DISCORD_WEBHOOK_URL", "")

RSS_FEEDS = [
    "https://www.seznamzpravy.cz/rss",
    "https://www.novinky.cz/rss",
    "https://cnn.iprima.cz/rss",
    "https://ct24.ceskatelevize.cz/rss",
    "https://www.irozhlas.cz/rss/irozhlas",
    "https://www.denik.cz/rss/zpravy.html",
    "https://servis.idnes.cz/rss.aspx?c=zpravodaj",
    "https://zpravy.aktualne.cz/rss/?lp=1",
    "https://www.ceskenoviny.cz/sluzby/rss/zpravy.php",
    "https://servis.lidovky.cz/rss.aspx",
    "https://www.tyden.cz/rss",
    "https://www.info.cz/rss",
    "https://denikn.cz/rss",
    "https://hn.cz/?m=rss",
    "https://www.e15.cz/rss",
    "https://www.penize.cz/rss",
    "https://www.kurzy.cz/zpravy/util/forext.dat?type=rss",
    "https://cc.cz/feed/",
    "https://vtm.zive.cz/rss",
    "https://www.zive.cz/rss/",
    "https://www.lupa.cz/rss/aktuality/",
    "https://www.reflex.cz/rss",
    "https://sport.ceskatelevize.cz/rss",
]

SCRAPE_HOURS_BACK = 25
MAX_ENTRIES_PER_FEED = 0
MAX_ARTICLE_CHARS = 6000
SCRAPE_WORKERS = 12
SCRAPE_TIMEOUT = 8
CHUNK_CHARS = 60_000
MINI_MODEL = "gpt-5-mini"
PREMIUM_MODEL = "gpt-5.4"
NUM_PICK = 20
NUM_NUMBER = 20
OUTPUT_DIR = "daily_questions"
OUTPUT_FILENAME = "questions_{date}.json"
API_TIMEOUT = 300
API_RETRIES = 3
CLEANUP_DAYS = 7
PREMIUM_TOKEN_BUDGET = 200_000  # Stay safely under 250K free limit per key

client1 = OpenAI(api_key=OPENAI_API_KEY, timeout=API_TIMEOUT)
client2 = OpenAI(api_key=OPENAI_API_KEY2, timeout=API_TIMEOUT) if OPENAI_API_KEY2 else None

# Track premium model token usage per client
_token_usage = {"client1": 0, "client2": 0}


def _get_client_and_name(prefer_secondary: bool = False) -> tuple:
    """Return (client, name) — uses secondary if preferred and available."""
    if prefer_secondary and client2 and _token_usage["client2"] < PREMIUM_TOKEN_BUDGET:
        return client2, "client2"
    if _token_usage["client1"] < PREMIUM_TOKEN_BUDGET:
        return client1, "client1"
    if client2 and _token_usage["client2"] < PREMIUM_TOKEN_BUDGET:
        return client2, "client2"
    # Both exhausted — return client1 anyway (will likely hit billing)
    log(f"  [WARN] Both API key budgets exhausted! client1: {_token_usage['client1']:,}, client2: {_token_usage['client2']:,}")
    return client1, "client1"


def preflight_check():
    """Verify OpenAI API connections."""
    log("Checking API connections...")

    for name, c in [("client1", client1), ("client2", client2)]:
        if c is None:
            log(f"  [SKIP] {name} — no API key configured")
            continue
        for model in (PREMIUM_MODEL, MINI_MODEL):
            try:
                c.chat.completions.create(
                    model=model,
                    messages=[{"role": "user", "content": "hi"}],
                    max_completion_tokens=5,
                )
                log(f"  [OK] {name} ({model})")
            except Exception as e:
                log(f"  [ERR] {name} ({model}) - {type(e).__name__}: {e}")

    log("")


def api_call_with_retry(func, *args, **kwargs):
    """Call API function with automatic retry and exponential backoff."""
    for attempt in range(1, API_RETRIES + 1):
        try:
            return func(*args, **kwargs)
        except Exception as e:
            log(f"  [WARN] Attempt {attempt}/{API_RETRIES}: {type(e).__name__}: {e}")
            if attempt == API_RETRIES:
                raise
            time.sleep(5 * attempt)


# Pydantic schemas
class Question(BaseModel):
    content: str = Field(description="Question text.")
    questionType: Literal["pick", "number"] = Field(description="Question type.")
    correctAnswer: str = Field(description="The correct answer (integer string for number type).")
    wrongAnswers: Optional[List[str]] = Field(default_factory=list, description="3 fake answers (only for pick type).")


class QuizResponse(BaseModel):
    questions: List[Question]


# Helpers & Logging
os.makedirs(OUTPUT_DIR, exist_ok=True)
DEBUG_LOG_FILE = os.path.join(OUTPUT_DIR, f"debug_{datetime.now().strftime('%Y-%m-%d_%H-%M-%S')}.log")


def log(msg):
    timestamp = datetime.now().strftime('%H:%M:%S')
    line = f"[{timestamp}] {msg}"
    print(line)
    with open(DEBUG_LOG_FILE, "a", encoding="utf-8") as f:
        f.write(line + "\n")


# Paywall and clutter removal
PAYWALL_PHRASES = [
    "Zajímají vás další kvalitní článk", "Chcete vědět, co se děje",
    "Odebírejte nejlepší newsletter", "Abyste mohli pokračovat",
    "Tento článek je zamčený", "Předplaťte si", "Přihlaste se pro přístup",
    "Registrujte se zdarma", "Tento obsah je dostupný pouze",
    "Získejte přístup k celému článku", "Vyzkoušejte Premium",
    "Už mám předplatné", "Přihlásit se přes", "Pokračujte ve čtení",
]

_GARBAGE_PATTERNS = [
    (re.compile(r"Přejít k obsahu.*?východiska", re.S), ""),
    (re.compile(r"ISSN: \d{4}-\d{4}.*?play", re.S), ""),
    (re.compile(r"(ČTĚTE TAKÉ|MOHLO BY VÁS ZAJÍMAT|DÁLE ČTĚTE|Přečtěte si také).*?(\n|$)", re.IGNORECASE), ""),
    (re.compile(r"Zdroj: [A-Za-z0-9, /\.\-]+"), ""),
    (re.compile(r"(Souhlas s cookies|Používáme cookies|Nastavení cookies).*?(\n|$)", re.IGNORECASE), ""),
    (re.compile(r"(Reklama|Inzerce|Komerční sdělení)\s*", re.IGNORECASE), ""),
    (re.compile(r"\s+"), " "),
]


def clean_garbage(text: str) -> str:
    """Remove known paywall sentences and clutter patterns.

    Paywall phrases are surgically removed (the sentence containing them)
    rather than truncating everything after the first match.
    """
    if not text:
        return ""

    # Remove individual sentences that contain paywall phrases
    for phrase in PAYWALL_PHRASES:
        if phrase not in text:
            continue
        # Walk through all occurrences and blank the surrounding sentence
        parts = text.split(phrase)
        cleaned_parts = []
        for i, part in enumerate(parts):
            if i == 0:
                # Keep everything up to the last sentence boundary before the phrase
                last_dot = max(part.rfind(". "), part.rfind("! "), part.rfind("? "))
                cleaned_parts.append(part[:last_dot + 1] if last_dot > 0 else part)
            else:
                # Skip to the first sentence boundary after the phrase
                first_dot = min(
                    (part.find(". ") if part.find(". ") >= 0 else len(part)),
                    (part.find("! ") if part.find("! ") >= 0 else len(part)),
                    (part.find("? ") if part.find("? ") >= 0 else len(part)),
                )
                cleaned_parts.append(part[first_dot + 2:] if first_dot < len(part) else "")
        text = " ".join(p for p in cleaned_parts if p.strip())

    for pattern, replacement in _GARBAGE_PATTERNS:
        text = pattern.sub(replacement, text)
    return text.strip()


def get_entry_date(entry) -> datetime | None:
    """Extract and parse the publication datetime from an RSS entry."""
    for field in ("published_parsed", "updated_parsed"):
        tp = entry.get(field)
        if tp:
            try:
                return datetime.fromtimestamp(timegm(tp), tz=timezone.utc)
            except Exception:
                pass
    for field in ("published", "updated"):
        raw = entry.get(field, "")
        if raw:
            try:
                return parsedate_to_datetime(raw)
            except Exception:
                pass
    return None


def is_recent(entry, cutoff: datetime) -> bool:
    """Check if the article was published after the cutoff time."""
    dt = get_entry_date(entry)
    if dt is None:
        return False
    if dt.tzinfo is None:
        dt = dt.replace(tzinfo=timezone.utc)
    return dt >= cutoff


# Scraping
SESSION = requests.Session()
SESSION.headers.update({
    "User-Agent": "Mozilla/5.0 (X11; Linux x86_64; rv:128.0) Gecko/20100101 Firefox/128.0",
    "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8",
    "Accept-Language": "cs,en;q=0.5",
})

# Scrape error tracking for diagnostics — tracks per domain
_scrape_errors: dict[str, Counter] = {}


def _track_error(url: str, error_type: str):
    """Record a scrape error for the given URL's domain."""
    try:
        domain = url.split("/")[2]
    except (IndexError, AttributeError):
        domain = "unknown"
    if domain not in _scrape_errors:
        _scrape_errors[domain] = Counter()
    _scrape_errors[domain][error_type] += 1


def scrape_article_text(url: str) -> str:
    """Download and extract raw text from an article URL."""
    try:
        res = SESSION.get(url, timeout=SCRAPE_TIMEOUT, allow_redirects=True)
        res.raise_for_status()
        if "text/html" not in res.headers.get("Content-Type", ""):
            _track_error(url, "non_html")
            return ""
        soup = BeautifulSoup(res.text, "html.parser")
        for tag in soup.find_all(["script", "style", "nav", "footer", "aside", "header", "form", "iframe"]):
            tag.decompose()
        paragraphs = (soup.find("article") or soup).find_all("p")
        texts = [p.get_text().strip() for p in paragraphs]
        text = " ".join(t for t in texts if len(t) > 20)
        return text[:MAX_ARTICLE_CHARS] if MAX_ARTICLE_CHARS else text
    except requests.Timeout:
        _track_error(url, "timeout")
        return ""
    except requests.HTTPError as e:
        _track_error(url, f"http_{e.response.status_code}")
        return ""
    except requests.ConnectionError:
        _track_error(url, "connection")
        return ""
    except Exception as e:
        _track_error(url, type(e).__name__)
        return ""


def scrape_single_entry(entry) -> dict | None:
    """Process a single RSS entry into structured content."""
    content = clean_garbage(scrape_article_text(entry.get("link", "")))
    if len(content) < 200:
        content = clean_garbage(entry.get("summary", ""))
    if len(content) < 100:
        return None
    return {"title": entry.get("title", ""), "content": content}


def fetch_daily_news() -> str:
    """Download fresh articles from RSS feeds and format them as text."""
    log("[1/6] Downloading fresh articles...")
    cutoff = datetime.now(timezone.utc) - timedelta(hours=SCRAPE_HOURS_BACK)
    log(f"  [INFO] Articles since {cutoff.strftime('%Y-%m-%d %H:%M')} UTC ({SCRAPE_HOURS_BACK}h back)")

    entries_to_scrape = []
    for url in RSS_FEEDS:
        source = url.split("/")[2]
        try:
            res = SESSION.get(url, timeout=SCRAPE_TIMEOUT)
            feed = feedparser.parse(res.content)
            recent = [e for e in feed.entries if is_recent(e, cutoff)]
            if MAX_ENTRIES_PER_FEED:
                recent = recent[:MAX_ENTRIES_PER_FEED]
            log(f"  [OK] {source}: {len(recent)}/{len(feed.entries)}")
            entries_to_scrape.extend(recent)
        except Exception as e:
            log(f"  [ERR] {source}: {e}")

    log(f"  [INFO] {len(entries_to_scrape)} articles to download...")

    _scrape_errors.clear()
    results: list[dict] = []
    seen_hashes: set[str] = set()
    with ThreadPoolExecutor(max_workers=SCRAPE_WORKERS) as pool:
        futures = {pool.submit(scrape_single_entry, e): e for e in entries_to_scrape}
        for future in as_completed(futures):
            result = future.result()
            if not result:
                continue
            # Hash beginning + middle + end for better collision resistance on wire stories
            c = result["content"]
            sample = c[:300] + c[len(c) // 2 : len(c) // 2 + 300] + c[-300:]
            h = hashlib.md5(sample.encode()).hexdigest()
            if h in seen_hashes:
                continue
            seen_hashes.add(h)
            results.append(result)

    if _scrape_errors:
        total_errors = sum(sum(c.values()) for c in _scrape_errors.values())
        log(f"  [INFO] Scrape errors: {total_errors} total across {len(_scrape_errors)} domains:")
        for domain in sorted(_scrape_errors, key=lambda d: sum(_scrape_errors[d].values()), reverse=True):
            log(f"    {domain}: {dict(_scrape_errors[domain])}")

    before = len(results)
    results = _title_dedup(results)
    log(f"  [INFO] {before} articles -> {len(results)} after deduplication (content + title)")

    data = "\n\n".join(f"TITLE: {r['title']}\nCONTENT: {r['content']}\n---" for r in results)
    log(f"  [INFO] {len(data):,} characters gathered")
    return data


def _title_words(title: str) -> set[str]:
    """Normalize title to a word set (lowercase, no punctuation, ignore short words)."""
    words = re.sub(r"[^\w\s]", "", title.lower()).split()
    return {w for w in words if len(w) > 2}


def _title_dedup(articles: list[dict], threshold: float = 0.40) -> list[dict]:
    """Merge articles with similar titles. Combines content, keeps longest title."""
    kept: list[dict] = []
    kept_words: list[set[str]] = []
    for article in articles:
        words = _title_words(article["title"])
        if not words:
            kept.append(article)
            kept_words.append(set())
            continue

        merged = False
        for i, existing_words in enumerate(kept_words):
            if not existing_words:
                continue
            overlap = len(words & existing_words) / len(words | existing_words)
            if overlap >= threshold:
                existing = kept[i]
                if len(article["title"]) > len(existing["title"]):
                    existing["title"] = article["title"]
                if article["content"] not in existing["content"]:
                    combined = existing["content"] + " " + article["content"]
                    existing["content"] = combined[:MAX_ARTICLE_CHARS]
                kept_words[i] = existing_words | words
                merged = True
                break

        if not merged:
            kept.append(article)
            kept_words.append(words)
    return kept


MINI_SYSTEM_PROMPT = """\
You are a precise data analyst. Your ONLY task is to extract hard facts from the text.
The output format is bullet-point Markdown, one fact = one line starting with "- ".
OUTPUT LANGUAGE MUST BE CZECH.

STRICT RULES:
1. Extract ONLY verifiable facts: specific numbers, names, dates, places, results, scores, statistics, prices.
2. ABSOLUTELY NO HALLUCINATIONS. Do not embellish or interpret anything.
   If the text says "fire" - write "fire" (or "požár"), NOT "attack".
   If there is no specific number in the text - DO NOT invent one.
3. PRESERVE EXACT CONTEXT: Who, what, where, when, why, how. Do not abbreviate at the expense of accuracy.
   Wrong: "41 mrtvých v baru ve Švýcarsku"
   Right: "Při požáru v baru Le Constellation v Crans-Montana ve Švýcarsku zahynulo 41 lidí, příčinou byla zábavní pyrotechnika"
4. PRESERVE THE NATURE OF EVENT: Distinguish attack vs accident, fire vs explosion, murder vs death.
5. EVERY FACT MUST BE UNIQUE - do not repeat the same information in different words.
6. Topics: politics, economy, sports, tech, culture, science, society - everything is relevant.
7. Filter ONLY: extreme graphical violence and explicit sexual content.
8. NO introductions, summaries, comments, personal opinions. JUST raw facts.
9. Cover the entire input uniformly from start to finish."""


def extract_facts(raw_text: str) -> str:
    """Extract hard facts from articles via GPT chunking."""
    log("[2/6] Fact Extraction...")
    chunks: list[str] = []
    for i in range(0, len(raw_text), CHUNK_CHARS):
        chunk = raw_text[i : i + CHUNK_CHARS]
        if i + CHUNK_CHARS < len(raw_text):
            sep = chunk.rfind("---")
            if sep > CHUNK_CHARS * 0.5:
                chunk = chunk[: sep + 3]
            else:
                nl = chunk.rfind("\n")
                if nl > CHUNK_CHARS * 0.7:
                    chunk = chunk[:nl]
        chunks.append(chunk)

    log(f"  [INFO] {len(chunks)} chunk(s) - starting parallel extraction...")

    def process_chunk(idx, chunk):
        resp = api_call_with_retry(
            client1.chat.completions.create,
            model=MINI_MODEL,
            messages=[
                {"role": "system", "content": MINI_SYSTEM_PROMPT},
                {"role": "user", "content": f"Extract ALL facts.\n\nDATA:\n{chunk}"},
            ],
            max_completion_tokens=16_000,
        )
        content = resp.choices[0].message.content
        log(f"  [OK] Chunk {idx}/{len(chunks)} completed.")
        return content

    all_facts: list[str] = ["" for _ in chunks]
    with ThreadPoolExecutor(max_workers=min(10, len(chunks))) as pool:
        futures = {pool.submit(process_chunk, idx, chunk): idx for idx, chunk in enumerate(chunks, 1)}
        for future in as_completed(futures):
            idx = futures[future]
            try:
                all_facts[idx - 1] = future.result()
            except Exception as e:
                log(f"  [ERR] Chunk {idx} error: {e}")

    merged = "\n\n".join(filter(None, all_facts))
    log(f"  [INFO] Facts: {len(merged):,} total characters after extraction.")
    return merged


# ================= STEP 3: CATEGORIZE & PRIORITIZE =================
CATEGORIZE_SYSTEM_PROMPT = """\
You are a news editor preparing material for a pub quiz. Your task is to take a raw list of facts and produce a STRUCTURED, CATEGORIZED, DEDUPLICATED summary.

OUTPUT LANGUAGE: CZECH.

PROCESS:
1. Read ALL input facts from start to finish.
2. Remove exact duplicates and near-duplicates (same event described differently — keep the most detailed version).
3. Assign each fact to exactly one category.
4. Within each category, sort facts by IMPACT and INTERESTINGNESS:
   - TOP: Events affecting many people, shocking statistics, viral/bizarre incidents, major decisions, record-breaking numbers.
   - BOTTOM: Routine events, minor updates, expected outcomes.

MANDATORY CATEGORIES (use all that have facts):
## POLITIKA
## EKONOMIKA
## SPORT
## TECHNOLOGIE
## KULTURA A MÉDIA
## VĚDA A ZDRAVÍ
## SPOLEČNOST A KRIMINALITA
## SVĚT

FORMAT:
- Use Markdown headers (##) for categories.
- Each fact is one bullet point starting with "- ".
- Keep ALL meaningful facts — do NOT summarize or shorten them. Preserve names, numbers, places, dates.
- Merge duplicates by combining details into one richer bullet point.
- If a fact contains multiple distinct pieces of information, keep them together.

RULES:
- NO hallucinations. Do not add any information that is not in the input.
- NO commentary, no introductions, no conclusions.
- Preserve the EXACT nature of events (fire ≠ attack, death ≠ murder, resignation ≠ firing).
- Every fact from the input must appear in the output (unless it's a duplicate)."""


def _merge_categorized_chunks(chunk_results: list[str]) -> str:
    """Merge multiple categorized outputs by concatenating facts under each category header.
    
    Flexibly handles various LLM output formats:
    - Headers: ## Title, # Title, ### Title, **Title**, ALL CAPS TITLE
    - Bullets: - fact, * fact, • fact, 1. fact, 1) fact
    Falls back to raw concatenation if parsing finds nothing.
    """
    categories: OrderedDict[str, list[str]] = OrderedDict()
    current_cat = "## OSTATNÍ"
    
    # Regex for detecting headers
    header_re = re.compile(
        r"^(?:#{1,4}\s+(.+?)#*\s*$|"               # ## Header ## or ## Header
        r"\*\*\s*(.+?)\s*\*\*\s*$|"                  # **Header**
        r"([A-ZÁČĎÉĚÍŇÓŘŠŤÚŮÝŽ ]{5,})\s*:?\s*$)"    # ALL CAPS HEADER (Czech)
    )
    # Regex for detecting bullet points
    bullet_re = re.compile(r"^\s*(?:[-*•–]|\d+[.)]\s)\s*(.+)")
    
    for chunk_text in chunk_results:
        for line in chunk_text.split("\n"):
            stripped = line.strip()
            if not stripped:
                continue
            
            # Try header match
            header_match = header_re.match(stripped)
            if header_match:
                cat_name = next((g for g in header_match.groups() if g is not None), "").strip()
                if cat_name:
                    current_cat = f"## {cat_name.upper()}"
                    if current_cat not in categories:
                        categories[current_cat] = []
                    continue
            
            # Try bullet match
            bullet_match = bullet_re.match(stripped)
            if bullet_match:
                fact_text = bullet_match.group(1).strip()
                if len(fact_text) > 10:
                    if current_cat not in categories:
                        categories[current_cat] = []
                    categories[current_cat].append(f"- {fact_text}")
    
    # Dedup within each category
    result_lines = []
    for cat, facts in categories.items():
        seen = set()
        unique_facts = []
        for fact in facts:
            normalized = fact.lower().strip()
            if normalized not in seen:
                seen.add(normalized)
                unique_facts.append(fact)
        if unique_facts:
            result_lines.append(f"\n{cat}")
            result_lines.extend(unique_facts)
    
    merged = "\n".join(result_lines)
    
    # Fallback: if parser found almost nothing, just concatenate raw chunks
    total_input_lines = sum(1 for c in chunk_results for l in c.split("\n") if l.strip())
    parsed_facts = sum(1 for l in result_lines if l.strip().startswith("- "))
    
    if parsed_facts < total_input_lines * 0.1 and total_input_lines > 20:
        log(f"  [WARN] Parser only found {parsed_facts}/{total_input_lines} lines — format mismatch, using raw concatenation")
        return "\n\n---\n\n".join(chunk_results)
    
    return merged


def categorize_facts(raw_facts: str) -> str:
    """Categorize, deduplicate and prioritize extracted facts via gpt-5-mini."""
    log("[3/6] Categorizing & Prioritizing Facts...")
    
    # Split into chunks if facts are very long
    chunks: list[str] = []
    lines = raw_facts.split("\n")
    current_chunk: list[str] = []
    current_len = 0
    
    for line in lines:
        line_len = len(line)
        if current_len + line_len > CHUNK_CHARS and current_chunk:
            chunks.append("\n".join(current_chunk))
            current_chunk = []
            current_len = 0
        current_chunk.append(line)
        current_len += line_len
    if current_chunk:
        chunks.append("\n".join(current_chunk))
    
    log(f"  [INFO] {len(chunks)} chunk(s), categorizing in parallel...")
    
    def categorize_chunk(idx, chunk):
        resp = api_call_with_retry(
            client1.chat.completions.create,
            model=MINI_MODEL,
            messages=[
                {"role": "system", "content": CATEGORIZE_SYSTEM_PROMPT},
                {"role": "user", "content": f"Categorize and prioritize ALL these facts:\n\n{chunk}"},
            ],
            max_completion_tokens=32_000,
        )
        content = resp.choices[0].message.content
        finish = resp.choices[0].finish_reason
        
        if not content:
            log(f"  [WARN] Chunk {idx}/{len(chunks)}: empty response (finish_reason={finish})")
            return ""
        
        # Strip markdown code fences that LLM sometimes wraps output in
        content = re.sub(r"^```(?:markdown|md)?\s*\n?", "", content)
        content = re.sub(r"\n?```\s*$", "", content)
        
        log(f"  [OK] Chunk {idx}/{len(chunks)}: {len(content):,} chars (finish_reason={finish})")
        return content

    chunk_results: list[str] = ["" for _ in chunks]
    with ThreadPoolExecutor(max_workers=min(10, len(chunks))) as pool:
        futures = {pool.submit(categorize_chunk, idx, chunk): idx for idx, chunk in enumerate(chunks, 1)}
        for future in as_completed(futures):
            idx = futures[future]
            try:
                chunk_results[idx - 1] = future.result()
            except Exception as e:
                log(f"  [ERR] Categorize chunk {idx} error: {e}")

    # Merge in Python — no LLM, no data loss
    non_empty = [c for c in chunk_results if c]
    
    if not non_empty:
        log(f"  [WARN] All categorization chunks returned empty! Using raw facts.")
        return raw_facts
    
    # Debug: log first 200 chars of each chunk so we can see the format
    for i, c in enumerate(non_empty):
        preview = c[:200].replace("\n", " | ")
        log(f"  [DEBUG] Chunk {i+1}/{len(non_empty)} preview: {preview}...")
    
    result = _merge_categorized_chunks(non_empty)

    # Count categories and facts
    categories_found = [l for l in result.split("\n") if l.strip().startswith("## ")]
    fact_lines = [l for l in result.split("\n") if l.strip().startswith("- ")]
    log(f"  [OK] {len(fact_lines)} facts in {len(categories_found)} categories ({len(result):,} chars)")
    
    # Sanity check: if we lost too much data, fall back to raw facts
    raw_bullet_count = raw_facts.count("\n- ")
    if len(fact_lines) < 50 or (raw_bullet_count > 0 and len(fact_lines) < raw_bullet_count * 0.15):
        log(f"  [WARN] Only {len(fact_lines)} facts survived (raw had ~{raw_bullet_count} bullets) — too few, using raw facts")
        return raw_facts
    
    return result


# ================= STEP 4: QUESTION GENERATION =================
def _build_quiz_prompt():
    now = datetime.now()
    return f"""\
You are a creator of a DIFFICULT pub quiz for smart people. Today's date is {now.strftime("%d. %m. %Y")}. 

The input contains CATEGORIZED FACTS from today's news, organized by category (## headers) with the most impactful facts listed first in each category. USE THIS STRUCTURE — pick questions from ALL categories.

═══════════════════════════════════════════════
★★★ CRITICAL: FULL COVERAGE ★★★
═══════════════════════════════════════════════
You MUST read the ENTIRE input from start to finish BEFORE generating any questions.
DO NOT generate questions while reading — first read everything, then pick the best facts.

The facts are organized into CATEGORIES with ## headers. You MUST use facts from ALL categories, not just the first few.

MANDATORY PROCESS:
1. Read ALL categories and ALL facts completely.
2. From EACH category, identify the most surprising/specific facts with concrete details (numbers, names, places).
3. Ensure you have candidate questions from every category before writing anything.
4. Write questions, distributing them evenly across categories.

QUANTITIES (strictly follow!):
- EXACTLY {NUM_PICK}x "pick" (multiple choice from 4 options) - do these FIRST.
- EXACTLY {NUM_NUMBER}x "number" (answer is an integer) - do these NEXT.
- TOTAL {NUM_PICK + NUM_NUMBER} questions.

FORMAT:
- pick: correctAnswer + exactly 3 believable wrongAnswers.
- number: correctAnswer is an INTEGER as a string (no decimals!). wrongAnswers = [].

═══════════════════════════════════════════════
CATEGORY BALANCE (MANDATORY — VIOLATION = FAILURE):
═══════════════════════════════════════════════
You MUST cover AT LEAST 6 different categories out of these 8:
  sport, politics, economy, tech, culture/media, science, society/crime, world

HARD LIMITS:
- MAX 4 questions from any single category.
- MIN 1 question from at least 6 categories.
- PRIORITIZE events with large public impact: widely discussed stories, shocking statistics, major decisions affecting many people, viral/bizarre incidents. These make the best quiz questions because players are likely to have heard about them but not remember the details.
- Do NOT exhaust your question budget on war/politics/economy alone. Spread it out.

═══════════════════════════════════════════════
DIFFICULTY:
═══════════════════════════════════════════════
Questions MUST be HARD. Goal: average person answers max 30-40% correctly.
A good question requires knowledge of a SPECIFIC detail from the text that a normal person wouldn't know.

★ BANNED QUESTION TYPES (generating a single one is a FATAL FAILURE):

1. SELF-ANSWERING QUESTIONS (answer is in the question):
   ✗ "Which CZECH team beat the Japanese?" -> "Czech team" (answer IS in the question!)
   ✓ "Which team won the friendly para ice hockey match against Japan?" -> "Czechia" (wrongAnswers: Canada, Finland, Sweden)

2. BINARY / TRIVIAL QUESTIONS (can be deduced logically):
   ✗ "Who won the Slavia-Sparta derby?" -> only 2 real options
   ✓ "What was the final score of the Prague football derby?" -> "2:1" (wrongAnswers: 1:0, 3:2, 0:0)

3. GENERALLY KNOWN QUESTIONS (everyone knows without reading news):
   ✗ "Who is the US president?" -> trivial
   ✓ "What percentage of tariffs did the US impose on Chinese goods?" -> specific number

4. VAGUE / TOO BROAD QUESTIONS:
   ✗ "Which country is facing problems?" -> too vague
   ✓ "In which Swiss town did the deadly nightclub fire break out?" -> specific detail

★ HOW TO CREATE A GOOD QUESTION:
- Ask for a SPECIFIC detail: person's name, place name, exact number, specific result.
- Ask about secondary details, not WHAT happened, but WHERE, WHEN, AT WHAT COST.

═══════════════════════════════════════════════
★★★ MANDATORY: FULL CONTEXT IN EVERY QUESTION ★★★
═══════════════════════════════════════════════
Every question MUST be self-contained. A reader who hasn't seen the news must understand WHAT DOMAIN the question is about just from reading it.

REQUIRED CONTEXT (include whichever apply):
- SPORT: Always name the specific sport/discipline (football, ice hockey, tennis, biathlon...) and competition/league if relevant.
- POLITICS: Name the country or institution.
- ECONOMY: Name the company, sector, or country.
- SCIENCE/TECH: Name the field or technology.
- CULTURE: Name the medium (film, book, music, TV...).
- CRIME/SOCIETY: Name the city/region and type of event.

★ EXAMPLES:
  ✗ "Jaké bylo skóre zápasu Sparta vs Plzeň?" (which sport??)
  ✓ "Jaké bylo skóre fotbalového zápasu Sparta vs Plzeň v české lize?"

  ✗ "Kdo vyhrál turnaj v Indian Wells?" (which sport??)
  ✓ "Kdo vyhrál tenisový turnaj WTA v Indian Wells?"

  ✗ "Kolik lidí zemřelo při útoku?" (where? what kind?)
  ✓ "Kolik lidí zemřelo při raketovém útoku na Charkov na Ukrajině?"

  ✗ "Jaký výsledek měl zápas s Finskem?" (what sport? what competition?)
  ✓ "Jakým výsledkem skončil zápas české hokejové reprezentace s Finskem na Channel One Cupu?"

QUALITY RULES:
- FACTUAL ACCURACY is CRITICAL. Do not mix up sports, names, disciplines.
- NO YEARS IN QUESTIONS: Questions are daily, so current year is implied.
- NO DUPLICATES: Each question MUST be about a different topic/event.

RULES FOR PICK QUESTIONS:
- All 4 answers MUST be BELIEVABLE.
- NEVER use answers that logically make no sense in the context.
- Correct + wrong answers must be from the SAME category (footballers, cities, etc.).

RULES FOR NUMBER QUESTIONS:
- Answer is an INTEGER as string. wrongAnswers = [].
- BANNED: 0, 1, 2 (trivial) and over 100 000 (unguessable).
- IDEAL range: 3 - 100 000. Age, score, percentages, prices in thousands, attendance.

STYLE & LANGUAGE:
- ALL EXPORTED QUESTIONS AND ANSWERS MUST BE STRICTLY IN CZECH LANGUAGE.
- Concise, human, with humor where appropriate.
- No parentheses, no references to "article" or "text".

FINAL CHECK (perform before submitting!):
1. Is the answer enclosed right in the text of the question? -> YES means delete it.
2. Could an average person guess it without reading news? -> YES means delete it.
3. Does the question realistically have only 2 possible answers? -> YES means rephrase it.
4. Do I have questions from at least 6 different categories? -> NO means replace duplicates from overrepresented categories.
5. Can the reader tell the domain/sport/country/field from the question alone? -> NO means add the missing context."""


def _question_words(text: str) -> set[str]:
    """Normalize question into a set of words for semantic deduplication."""
    words = re.sub(r"[^\w\s]", "", text.lower()).split()
    stop = {"kdo", "kde", "kdy", "jak", "jaký", "jaká", "jaké", "který", "která",
            "které", "kolik", "byl", "byla", "bylo", "pro", "při", "pod",
            "nad", "mezi", "nebo", "ale", "tak", "již", "jen", "ještě"}
    return {w for w in words if len(w) > 2 and w not in stop}


def _validate_questions(questions: list[dict]) -> list[dict]:
    """Filter out duplicate and low-quality questions."""
    seen_contents: set[str] = set()
    seen_answers: set[str] = set()
    kept_questions: list[tuple[set[str], dict]] = []
    valid: list[dict] = []

    nsfw_pattern = re.compile(
        r"\b(?:kure?v|p[ií]č|mrd|jeb|hovn|srát|srač|čurák|kokot|úchyl|porno|vražedn|sebevraž|znásiln)\b",
        re.IGNORECASE
    )

    for q in questions:
        # 0) NSFW check
        if nsfw_pattern.search(q["content"]):
            log(f"  [WARN] Blocked (NSFW filter): {q['content'][:60]}...")
            continue

        # 1) Exact textual duplication
        normalized = re.sub(r"\s+", " ", q["content"].lower().strip())
        if normalized in seen_contents:
            log(f"  [WARN] Duplicate (text): {q['content'][:60]}...")
            continue
        seen_contents.add(normalized)

        # 2) Semantic duplication (Jaccard similarity)
        q_words = _question_words(q["content"])
        is_semantic_dup = False
        if q_words:
            for existing_words, existing_q in kept_questions:
                if not existing_words:
                    continue
                overlap = len(q_words & existing_words) / len(q_words | existing_words)
                if overlap >= 0.55:
                    log(f"  [WARN] Duplicate (semantic): '{q['content'][:40]}' ~ '{existing_q['content'][:40]}'")
                    is_semantic_dup = True
                    break
        if is_semantic_dup:
            continue

        # 3) Answer duplication
        answer_key = f"{q['questionType']}:{q['correctAnswer'].strip().lower()}"
        if answer_key in seen_answers:
            log(f"  [WARN] Duplicate (answer '{q['correctAnswer']}'): {q['content'][:60]}...")
            continue
        seen_answers.add(answer_key)

        # 4) Pick format validation
        if q["questionType"] == "pick":
            wa = q.get("wrongAnswers", [])
            if not wa or len(wa) != 3:
                log(f"  [WARN] Invalid Pick format ({len(wa or [])} wrong answers): {q['content'][:60]}...")
                continue
            # Check correct answer not duplicated in wrong answers
            ca_lower = q["correctAnswer"].strip().lower()
            if ca_lower in {w.strip().lower() for w in wa}:
                log(f"  [WARN] Correct answer duplicated in wrongAnswers: {q['content'][:60]}...")
                continue

        # 5) Number format validation
        if q["questionType"] == "number":
            try:
                num = float(q["correctAnswer"])
                q["correctAnswer"] = str(int(round(num)))
                num_int = int(q["correctAnswer"])
                if num_int < 3 or num_int > 100_000:
                    log(f"  [WARN] Number out of bounds ({num_int}): {q['content'][:60]}...")
                    continue
            except (ValueError, TypeError):
                log(f"  [WARN] Number cannot be parsed: {q['correctAnswer']}")
                continue

        # 6) Self-answering check: if correctAnswer appears verbatim in the question
        if q["correctAnswer"].lower() in q["content"].lower() and len(q["correctAnswer"]) > 2:
            log(f"  [WARN] Self-answering (answer in question): {q['content'][:60]}...")
            continue

        kept_questions.append((q_words, q))
        valid.append(q)

    return valid


def _generate_gpt_questions(summary: str, quiz_prompt: str, use_secondary: bool = False) -> list[dict]:
    """Generate questions via GPT model using available client."""
    active_client, client_name = _get_client_and_name(prefer_secondary=use_secondary)
    log(f"  [INFO] Generating questions using {PREMIUM_MODEL} via {client_name}...")
    all_q: list[dict] = []

    for attempt in range(1, 4):
        need_total = (NUM_PICK + NUM_NUMBER) - len(all_q)
        if need_total <= 0:
            break

        if attempt > 1:
            log(f"  [INFO] GPT generation attempt {attempt}: requesting {need_total} questions...")
            prompt = f"I still need {need_total} questions. Generate ONLY these.\n\n{summary}"
        else:
            prompt = summary

        resp = api_call_with_retry(
            active_client.beta.chat.completions.parse,
            model=PREMIUM_MODEL,
            messages=[
                {"role": "system", "content": quiz_prompt},
                {"role": "user", "content": prompt},
            ],
            response_format=QuizResponse,
            max_completion_tokens=24_000,
            temperature=0.7,
        )

        parsed = resp.choices[0].message.parsed
        if parsed is None:
            log(f"  [WARN] GPT parsed=None (attempt {attempt}), skipping...")
            continue

        new_q = [q.model_dump() for q in parsed.questions]
        if resp.usage:
            tokens = resp.usage.total_tokens
            _token_usage[client_name] += tokens
            log(f"  [INFO] {client_name} tokens: {tokens:,} (budget: {_token_usage[client_name]:,}/{PREMIUM_TOKEN_BUDGET:,})")
        all_q.extend(new_q)
        log(f"  [INFO] GPT: +{len(new_q)} -> total {len(all_q)}")

    log(f"  [OK] Generated {len(all_q)} questions via {client_name}")
    return all_q


def generate_questions(summary: str) -> list[dict]:
    """Generate, validate and backfill questions via GPT."""
    _token_usage["client1"] = 0
    _token_usage["client2"] = 0
    budget_info = f"2x {PREMIUM_TOKEN_BUDGET:,}" if client2 else f"{PREMIUM_TOKEN_BUDGET:,}"
    log(f"[4/6] Generating Questions ({PREMIUM_MODEL}, budget: {budget_info} tokens)...")
    quiz_prompt = _build_quiz_prompt()

    judged = _generate_gpt_questions(summary, quiz_prompt)
    validated = _validate_questions(judged)

    final_pick = [q for q in validated if q["questionType"] == "pick"]
    final_num = [q for q in validated if q["questionType"] == "number"]

    for backfill in range(1, 7):
        need_p = NUM_PICK - len(final_pick)
        need_n = NUM_NUMBER - len(final_num)
        if need_p <= 0 and need_n <= 0:
            break

        # Pick best available client
        active_client, client_name = _get_client_and_name(prefer_secondary=True)
        
        parts = []
        if need_p > 0:
            parts.append(f"{need_p} pick")
        if need_n > 0:
            parts.append(f"{need_n} number")
        log(f"  [INFO] Backfill {backfill}/6: missing {' + '.join(parts)}, using {client_name}...")

        existing_topics = ', '.join(q['content'][:50] for q in final_pick + final_num)
        backfill_prompt = (
            f"I still need EXACTLY {' and '.join(parts)} questions. "
            f"Generate ONLY these, on DIFFERENT topics than what I already have: "
            f"{existing_topics}\n\n{summary}"
        )

        try:
            resp = api_call_with_retry(
                active_client.beta.chat.completions.parse,
                model=PREMIUM_MODEL,
                messages=[
                    {"role": "system", "content": quiz_prompt},
                    {"role": "user", "content": backfill_prompt},
                ],
                response_format=QuizResponse,
                max_completion_tokens=24_000,
                temperature=0.7,
            )

            if resp.usage:
                _token_usage[client_name] += resp.usage.total_tokens
                log(f"  [INFO] {client_name} budget: {_token_usage[client_name]:,}/{PREMIUM_TOKEN_BUDGET:,}")

            parsed = resp.choices[0].message.parsed
            if parsed is None:
                continue
            new_q = [q.model_dump() for q in parsed.questions]

            # Validate against ALL existing questions (not just the new batch)
            combined_for_validation = final_pick + final_num + new_q
            all_valid = _validate_questions(combined_for_validation)

            # Only keep the truly new ones that survived validation
            existing_set = {re.sub(r"\s+", " ", q["content"].lower().strip()) for q in final_pick + final_num}
            new_valid = [q for q in all_valid if re.sub(r"\s+", " ", q["content"].lower().strip()) not in existing_set]

            final_pick.extend(q for q in new_valid if q["questionType"] == "pick")
            final_num.extend(q for q in new_valid if q["questionType"] == "number")
            log(f"  [INFO] Backfill: +{len(new_valid)} -> {len(final_pick)}pick {len(final_num)}number")
        except Exception as e:
            log(f"  [ERR] Backfill error: {e}")

    # Strict verification
    final_pick = final_pick[:NUM_PICK]
    final_num = final_num[:NUM_NUMBER]
    final = final_pick + final_num

    if len(final_pick) < NUM_PICK or len(final_num) < NUM_NUMBER:
        log(f"  [WARN] TARGET NOT MET! Wanted {NUM_PICK}+{NUM_NUMBER}={NUM_PICK+NUM_NUMBER}, "
            f"got {len(final_pick)}+{len(final_num)}={len(final)} after 6 backfill attempts")
    else:
        log(f"  [OK] Target met: {len(final_pick)} pick + {len(final_num)} number = {len(final)} questions")

    return final


def upload_to_vyzyvatel(questions: list[dict]) -> None:
    """Upload generated questions in bulk to Vyzyvatel API."""
    if not VYZYVATEL_API_KEY or not VYZYVATEL_SET_ID:
        log("  [WARN] Vyzyvatel API key or Set ID not set, skipping upload.")
        return

    log(f"[5/6] Uploading {len(questions)} questions to Vyzyvatel (Set ID: {VYZYVATEL_SET_ID})")

    url = f"https://be.vyzyvatel.com/api/sets/{VYZYVATEL_SET_ID}/questions/batch"
    headers = {
        "Authorization": f"Bearer {VYZYVATEL_API_KEY}",
        "Content-Type": "application/json"
    }

    payload_questions = []
    for q in questions:
        pq = {
            "content": q["content"][:200],
            "questionType": q["questionType"],
            "correctAnswer": str(q["correctAnswer"])[:22] if q["questionType"] == "number" else str(q["correctAnswer"])[:100]
        }
        if q["questionType"] == "pick":
            pq["wrongAnswers"] = [str(wa)[:100] for wa in q.get("wrongAnswers", [])][:4]

        payload_questions.append(pq)

    batch_data = {"questions": payload_questions}

    try:
        response = requests.post(url, json=batch_data, headers=headers, timeout=30)
        if response.status_code == 201:
            result = response.json()
            log(f"  [OK] Successfully created {result.get('count', '?')} questions in the set!")
        else:
            log(f"  [ERR] Upload failed: {response.status_code} - {response.text}")
    except Exception as e:
        log(f"  [ERR] Fatal upload error: {type(e).__name__}: {e}")


def cleanup_old_questions(days_old: int = CLEANUP_DAYS) -> None:
    """Fetch questions from the set and delete those older than the specified number of days."""
    if not VYZYVATEL_API_KEY or not VYZYVATEL_SET_ID:
        log("  [WARN] Vyzyvatel API key or Set ID not set, skipping cleanup.")
        return

    log(f"[6/6] Cleaning up questions older than {days_old} days (Set ID: {VYZYVATEL_SET_ID})...")

    get_url = f"https://be.vyzyvatel.com/api/sets/{VYZYVATEL_SET_ID}/questions"
    delete_url = f"https://be.vyzyvatel.com/api/sets/{VYZYVATEL_SET_ID}/questions/batch"
    headers = {
        "Authorization": f"Bearer {VYZYVATEL_API_KEY}",
        "Content-Type": "application/json"
    }

    try:
        res = requests.get(get_url, headers=headers, timeout=30)
        if res.status_code != 200:
            log(f"  [ERR] Cannot fetch questions for cleanup: {res.status_code} - {res.text}")
            return

        data = res.json()
        questions = data if isinstance(data, list) else data.get("questions", [])

        if not questions:
            log("  [INFO] Set is empty, nothing to clean up.")
            return

        cutoff_date = datetime.now(timezone.utc) - timedelta(days=days_old)
        ids_to_delete = []

        for q in questions:
            q_id = q.get("id")
            updated_str = q.get("updatedAt")
            if not q_id or not updated_str:
                continue

            try:
                updated_dt = datetime.fromisoformat(str(updated_str).replace('Z', '+00:00'))
                if updated_dt.tzinfo is None:
                    updated_dt = updated_dt.replace(tzinfo=timezone.utc)
                if updated_dt < cutoff_date:
                    ids_to_delete.append(q_id)
            except Exception:
                pass

        if not ids_to_delete:
            log(f"  [OK] No questions older than {days_old} days found.")
            return

        log(f"  [INFO] Found {len(ids_to_delete)} old questions to delete.")

        for i in range(0, len(ids_to_delete), 500):
            chunk = ids_to_delete[i:i + 500]
            del_res = requests.delete(delete_url, json={"questionIds": chunk}, headers=headers, timeout=30)
            if del_res.status_code == 200:
                log(f"  [OK] Deleted {del_res.json().get('deletedCount', len(chunk))} old questions.")
            else:
                log(f"  [ERR] Delete error: {del_res.status_code} - {del_res.text}")

    except Exception as e:
        log(f"  [ERR] Fatal cleanup error: {type(e).__name__}: {e}")


# ================= DISCORD WEBHOOK =================
def send_discord_report(timings: dict, questions: list[dict], error_count: int, scrape_errors: dict, fact_count: int, category_count: int):
    """Send a pipeline summary embed to Discord webhook."""
    if not DISCORD_WEBHOOK_URL:
        return
    
    today = datetime.now().strftime("%Y-%m-%d")
    pick_count = sum(1 for q in questions if q["questionType"] == "pick")
    number_count = sum(1 for q in questions if q["questionType"] == "number")
    total_time = timings.get("total", 0)
    
    # Status
    if len(questions) >= 40 and error_count == 0:
        color = 0x2ECC71  # Green
        status = "✅ Success"
    elif len(questions) >= 35:
        color = 0xF39C12  # Orange
        status = "⚠️ Partial"
    else:
        color = 0xE74C3C  # Red
        status = "❌ Failed"
    
    # Scrape error summary
    scrape_lines = []
    for domain, errors in sorted(scrape_errors.items(), key=lambda x: sum(x[1].values()), reverse=True):
        scrape_lines.append(f"`{domain}`: {dict(errors)}")
    scrape_text = "\n".join(scrape_lines[:5]) if scrape_lines else "None"
    
    # Token usage
    token_lines = []
    for name, used in _token_usage.items():
        if name == "client2" and not client2:
            continue
        pct = round(used / PREMIUM_TOKEN_BUDGET * 100) if PREMIUM_TOKEN_BUDGET else 0
        bar = "█" * (pct // 10) + "░" * (10 - pct // 10)
        token_lines.append(f"`{name}`: {used:,} / {PREMIUM_TOKEN_BUDGET:,} ({pct}%) {bar}")
    token_text = "\n".join(token_lines) if token_lines else "N/A"
    
    embed = {
        "title": f"📰 Daily Quiz — {today}",
        "color": color,
        "fields": [
            {
                "name": "Status",
                "value": status,
                "inline": True,
            },
            {
                "name": "Questions",
                "value": f"**{len(questions)}**/40 ({pick_count} pick, {number_count} number)",
                "inline": True,
            },
            {
                "name": "Errors",
                "value": str(error_count),
                "inline": True,
            },
            {
                "name": "⏱️ Timing",
                "value": (
                    f"Scraping: `{timings.get('scrape', 0):.0f}s`\n"
                    f"Extraction: `{timings.get('extract', 0):.0f}s`\n"
                    f"Categorization: `{timings.get('categorize', 0):.0f}s`\n"
                    f"Generation: `{timings.get('generate', 0):.0f}s`\n"
                    f"**Total: `{total_time:.0f}s` ({total_time/60:.1f}min)**"
                ),
                "inline": False,
            },
            {
                "name": "📊 Data",
                "value": f"Facts: {fact_count} in {category_count} categories",
                "inline": True,
            },
            {
                "name": "🔑 Token Usage (gpt-5.4)",
                "value": token_text,
                "inline": False,
            },
        ],
        "timestamp": datetime.now(timezone.utc).isoformat(),
    }
    
    # Add scrape errors field only if there were errors
    if scrape_lines:
        embed["fields"].insert(5, {
            "name": "🔴 Scrape Errors",
            "value": scrape_text,
            "inline": False,
        })
    
    try:
        resp = requests.post(
            DISCORD_WEBHOOK_URL,
            json={"embeds": [embed]},
            timeout=10,
        )
        if resp.status_code in (200, 204):
            log(f"  [OK] Discord notification sent.")
        else:
            log(f"  [ERR] Discord webhook: {resp.status_code} - {resp.text[:100]}")
    except Exception as e:
        log(f"  [ERR] Discord webhook error: {type(e).__name__}: {e}")
SCHEDULE_HOUR = 11      # Pipeline starts at 11:45 Prague time
SCHEDULE_MINUTE = 45
PUBLISH_HOUR = 12       # Upload to Vyzyvatel at 12:00 Prague time
PUBLISH_MINUTE = 0
STATS_FILE = os.path.join(OUTPUT_DIR, "pipeline_stats.json")

try:
    from zoneinfo import ZoneInfo
except ImportError:
    from backports.zoneinfo import ZoneInfo

PRAGUE_TZ = ZoneInfo("Europe/Prague")


def _next_run_time() -> datetime:
    """Calculate the next scheduled run time (today or tomorrow at SCHEDULE_HOUR:SCHEDULE_MINUTE Prague time)."""
    now = datetime.now(PRAGUE_TZ)
    target = now.replace(hour=SCHEDULE_HOUR, minute=SCHEDULE_MINUTE, second=0, microsecond=0)
    if now >= target:
        target += timedelta(days=1)
    return target


def _wait_until_publish_time():
    """Sleep until PUBLISH_HOUR:PUBLISH_MINUTE Prague time. If already past, return immediately."""
    now = datetime.now(PRAGUE_TZ)
    target = now.replace(hour=PUBLISH_HOUR, minute=PUBLISH_MINUTE, second=0, microsecond=0)
    wait = (target - now).total_seconds()
    if wait > 0:
        log(f"  [INFO] Questions ready. Waiting {wait:.0f}s until {PUBLISH_HOUR:02d}:{PUBLISH_MINUTE:02d} to publish...")
        time.sleep(wait)
    else:
        log(f"  [INFO] Past publish time ({PUBLISH_HOUR:02d}:{PUBLISH_MINUTE:02d}), uploading immediately.")


def _load_stats() -> dict:
    """Load historical pipeline stats from JSON file."""
    try:
        with open(STATS_FILE, "r", encoding="utf-8") as f:
            return json.load(f)
    except (FileNotFoundError, json.JSONDecodeError):
        return {"runs": []}


def _save_stats(stats: dict):
    """Save pipeline stats, keeping last 30 runs."""
    stats["runs"] = stats["runs"][-30:]
    with open(STATS_FILE, "w", encoding="utf-8") as f:
        json.dump(stats, f, ensure_ascii=False, indent=2)


def _log_stats_summary(stats: dict):
    """Log average and min/max times from historical runs."""
    runs = stats.get("runs", [])
    if len(runs) < 2:
        return
    
    log(f"  [STATS] Historical averages ({len(runs)} runs):")
    
    for key, label in [
        ("total", "Total pipeline"),
        ("scrape", "Scraping"),
        ("extract", "Fact extraction"),
        ("categorize", "Categorization"),
        ("generate", "Question generation"),
    ]:
        values = [r.get(key, 0) for r in runs if r.get(key)]
        if not values:
            continue
        avg = sum(values) / len(values)
        lo, hi = min(values), max(values)
        log(f"    {label}: avg {avg:.0f}s | min {lo:.0f}s | max {hi:.0f}s")
    
    q_counts = [r.get("questions", 0) for r in runs if r.get("questions")]
    if q_counts:
        log(f"    Questions: avg {sum(q_counts)/len(q_counts):.1f} | min {min(q_counts)} | max {max(q_counts)}")
    
    err_counts = [r.get("errors", 0) for r in runs]
    if err_counts:
        log(f"    Errors per run: avg {sum(err_counts)/len(err_counts):.1f} | max {max(err_counts)}")


def run_pipeline():
    """Execute the full quiz pipeline once with step timing."""
    global DEBUG_LOG_FILE
    today = datetime.now().strftime("%Y-%m-%d")
    DEBUG_LOG_FILE = os.path.join(OUTPUT_DIR, f"debug_{datetime.now().strftime('%Y-%m-%d_%H-%M-%S')}.log")
    
    timings = {}
    error_count = 0
    fact_count = 0
    category_count = 0
    questions = []
    pipeline_start = time.time()

    log(f"=== Daily Quiz Pipeline - {today} ===")
    log(f"Debug log: {DEBUG_LOG_FILE}")
    
    # Load and show historical stats
    stats = _load_stats()
    _log_stats_summary(stats)
    
    preflight_check()

    # Step 1: Scrape
    t = time.time()
    raw = fetch_daily_news()
    timings["scrape"] = round(time.time() - t, 1)
    log(f"  [TIME] Scraping: {timings['scrape']}s")
    
    if not raw.strip():
        log("[ERR] No fresh articles found.")
        error_count += 1
        send_discord_report(timings, [], error_count, dict(_scrape_errors), 0, 0)
        return

    with open(os.path.join(OUTPUT_DIR, f"debug_1_raw_articles_{today}.txt"), "w", encoding="utf-8") as f:
        f.write(raw)

    # Step 2: Extract facts
    t = time.time()
    facts = extract_facts(raw)
    timings["extract"] = round(time.time() - t, 1)
    log(f"  [TIME] Fact extraction: {timings['extract']}s")
    
    if not facts.strip():
        log("[ERR] No facts extracted.")
        error_count += 1
        send_discord_report(timings, [], error_count, dict(_scrape_errors), 0, 0)
        return

    with open(os.path.join(OUTPUT_DIR, f"debug_2_extracted_facts_{today}.txt"), "w", encoding="utf-8") as f:
        f.write(facts)

    # Step 3: Categorize
    t = time.time()
    categorized = categorize_facts(facts)
    timings["categorize"] = round(time.time() - t, 1)
    log(f"  [TIME] Categorization: {timings['categorize']}s")
    
    if not categorized.strip():
        log("[ERR] Categorization produced no output, using raw facts.")
        categorized = facts
        error_count += 1

    # Count facts and categories from categorized output
    fact_count = sum(1 for l in categorized.split("\n") if l.strip().startswith("- "))
    category_count = sum(1 for l in categorized.split("\n") if l.strip().startswith("## "))

    with open(os.path.join(OUTPUT_DIR, f"debug_3_categorized_facts_{today}.txt"), "w", encoding="utf-8") as f:
        f.write(categorized)

    # Step 4: Generate questions
    t = time.time()
    questions = generate_questions(categorized)
    timings["generate"] = round(time.time() - t, 1)
    log(f"  [TIME] Question generation: {timings['generate']}s")

    out_file = os.path.join(OUTPUT_DIR, OUTPUT_FILENAME.format(date=today))
    with open(out_file, "w", encoding="utf-8") as f:
        json.dump({
            "date": today,
            "generated_at": datetime.now().isoformat(),
            "questions": questions,
        }, f, ensure_ascii=False, indent=2)
    log(f"  [INFO] Saved to {out_file}")

    # Wait for publish time (12:00) before uploading
    _wait_until_publish_time()

    # Step 5: Upload
    t = time.time()
    upload_to_vyzyvatel(questions)
    timings["upload"] = round(time.time() - t, 1)

    # Step 6: Cleanup
    cleanup_old_questions()

    # Final summary
    timings["total"] = round(time.time() - pipeline_start, 1)
    pick_count = sum(1 for q in questions if q["questionType"] == "pick")
    number_count = sum(1 for q in questions if q["questionType"] == "number")
    
    log(f"")
    log(f"  ┌─────────────────────────────────────┐")
    log(f"  │        PIPELINE SUMMARY              │")
    log(f"  ├─────────────────────────────────────┤")
    log(f"  │  Scraping:       {timings['scrape']:>7.1f}s           │")
    log(f"  │  Extraction:     {timings['extract']:>7.1f}s           │")
    log(f"  │  Categorization: {timings['categorize']:>7.1f}s           │")
    log(f"  │  Generation:     {timings['generate']:>7.1f}s           │")
    log(f"  │  Upload:         {timings.get('upload', 0):>7.1f}s           │")
    log(f"  │  ─────────────────────────           │")
    log(f"  │  TOTAL:          {timings['total']:>7.1f}s           │")
    log(f"  │  Questions:  {pick_count:>3}p + {number_count:>3}n = {len(questions):>3}/40  │")
    log(f"  │  Facts:      {fact_count:>4} in {category_count} categories  │")
    log(f"  │  Errors:         {error_count:>4}                │")
    for name, used in _token_usage.items():
        if name == "client2" and not client2:
            continue
        log(f"  │  {name}: {used:>7,} / {PREMIUM_TOKEN_BUDGET:,} tkn │")
    log(f"  └─────────────────────────────────────┘")
    
    # Save stats for historical tracking
    run_stats = {
        "date": today,
        "questions": len(questions),
        "errors": error_count,
        "facts": fact_count,
        "categories": category_count,
        "tokens_client1": _token_usage["client1"],
        "tokens_client2": _token_usage["client2"],
        **timings,
    }
    stats["runs"].append(run_stats)
    _save_stats(stats)
    
    # Send Discord notification
    send_discord_report(timings, questions, error_count, dict(_scrape_errors), fact_count, category_count)
    
    log(f"=== Completed at {datetime.now(PRAGUE_TZ).strftime('%H:%M:%S')} Prague time ===")


# Main Execution
if __name__ == "__main__":
    os.makedirs(OUTPUT_DIR, exist_ok=True)

    log(f"=== Quiz Scheduler Started ===")
    log(f"Schedule: pipeline at {SCHEDULE_HOUR:02d}:{SCHEDULE_MINUTE:02d}, publish at {PUBLISH_HOUR:02d}:{PUBLISH_MINUTE:02d} Prague time")

    def _today_lockfile() -> str:
        return os.path.join(OUTPUT_DIR, f".last_run_{datetime.now(PRAGUE_TZ).strftime('%Y-%m-%d')}")

    def _already_ran_today() -> bool:
        return os.path.exists(_today_lockfile())

    def _mark_ran_today():
        open(_today_lockfile(), "w").close()
        for f in os.listdir(OUTPUT_DIR):
            if f.startswith(".last_run_") and f != os.path.basename(_today_lockfile()):
                try:
                    os.remove(os.path.join(OUTPUT_DIR, f))
                except OSError:
                    pass

    # Run immediately on first start — but only if not already done today
    if _already_ran_today():
        log(f"  [INFO] Already ran today (lockfile exists). Skipping to scheduler.")
    else:
        try:
            run_pipeline()
            _mark_ran_today()
        except KeyboardInterrupt:
            log("\n[WARN] Interrupted by user.")
            exit(130)
        except Exception as e:
            log(f"[FATAL] Pipeline error: {type(e).__name__}: {e}")

    # Infinite scheduler loop
    while True:
        next_run = _next_run_time()
        wait_seconds = (next_run - datetime.now(PRAGUE_TZ)).total_seconds()
        log(f"=== Next run: {next_run.strftime('%Y-%m-%d %H:%M')} Prague time (sleeping {wait_seconds/3600:.1f}h) ===")

        try:
            time.sleep(max(0, wait_seconds))
            run_pipeline()
            _mark_ran_today()
        except KeyboardInterrupt:
            log("\n[WARN] Interrupted by user.")
            exit(130)
        except Exception as e:
            log(f"[FATAL] Pipeline error: {type(e).__name__}: {e}")
            continue
