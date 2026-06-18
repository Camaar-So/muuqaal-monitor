"""
Muuqaal Monitor — Somalia Media Intelligence Backend
Scrapes 15+ Somali news sources, translates, classifies, alerts clients.
Run: python app.py
Dashboard connects to: http://localhost:5000
"""

import os, sqlite3, hashlib, smtplib, schedule, threading, time, logging
from datetime import datetime, timezone
from email.mime.text import MIMEText
from email.mime.multipart import MIMEMultipart
from functools import wraps
from flask import Flask, request, jsonify
from dotenv import load_dotenv

load_dotenv()
logging.basicConfig(level=logging.INFO, format='%(asctime)s %(levelname)s %(message)s')
log = logging.getLogger(__name__)

try:
    import requests
    from bs4 import BeautifulSoup
    SCRAPING_AVAILABLE = True
except ImportError:
    SCRAPING_AVAILABLE = False
    log.warning("requests/bs4 not installed — scraping disabled")

app = Flask(__name__)

# ─── CONFIG ────────────────────────────────────────────────────────────────────
DB_PATH       = os.getenv('DB_PATH', 'muuqaal.db')
ADMIN_TOKEN   = os.getenv('ADMIN_TOKEN', 'change-me-in-env')
SMTP_HOST     = os.getenv('SMTP_HOST', 'smtp.gmail.com')
SMTP_PORT     = int(os.getenv('SMTP_PORT', 587))
SMTP_USER     = os.getenv('SMTP_USER', '')
SMTP_PASS     = os.getenv('SMTP_PASS', '')
FROM_EMAIL    = os.getenv('FROM_EMAIL', 'alerts@muuqaal.so')
HF_API_KEY    = os.getenv('HUGGINGFACE_API_KEY', '')

# ─── ALL SOMALI NEWS SOURCES ───────────────────────────────────────────────────
# 18 sources across Somalia, Somaliland, Puntland, diaspora
SOURCES = [
    # ── Major national outlets ──────────────────────────────────────────────
    {
        "name": "Garoweonline",
        "url": "https://www.garoweonline.com/en/news/somalia",
        "rss": "https://www.garoweonline.com/en/rss",
        "article_selector": "h3.entry-title a, h2.entry-title a",
        "region": "National",
        "lang": "en+so",
        "email": "news@garoweonline.com",
    },
    {
        "name": "Hiiraan Online",
        "url": "https://www.hiiraan.com/news4/",
        "rss": "https://www.hiiraan.com/rss/news4.xml",
        "article_selector": ".views-row a, h3 a",
        "region": "National",
        "lang": "en+so",
        "email": "info@hiiraan.com",
    },
    {
        "name": "Caasimada Online",
        "url": "https://www.caasimada.net/",
        "rss": "https://www.caasimada.net/feed/",
        "article_selector": "h2.entry-title a, h3.entry-title a",
        "region": "National",
        "lang": "so",
        "email": "caasimada@caasimada.net",
    },
    {
        "name": "Goobjoog News",
        "url": "https://goobjoog.com/english/",
        "rss": "https://goobjoog.com/english/feed/",
        "article_selector": "h3.jeg_post_title a, h2 a",
        "region": "National",
        "lang": "en+so",
        "email": "info@goobjoog.com",
    },
    {
        "name": "Dalsan News",
        "url": "https://www.dalsan.net/",
        "rss": "https://www.dalsan.net/feed/",
        "article_selector": "h3.entry-title a, .post-title a",
        "region": "National",
        "lang": "en+so",
        "email": "info@dalsan.net",
    },
    {
        "name": "Horseed Media",
        "url": "https://horseedmedia.net/",
        "rss": "https://horseedmedia.net/feed/",
        "article_selector": "h2.entry-title a, h3 a",
        "region": "National",
        "lang": "en+so",
        "email": "info@horseedmedia.net",
    },
    {
        "name": "Shabelle Media",
        "url": "https://shabelle.net/",
        "rss": "https://shabelle.net/feed/",
        "article_selector": "h3.entry-title a, h2 a",
        "region": "National",
        "lang": "so",
        "email": "info@shabelle.net",
    },
    {
        "name": "WardheerNews",
        "url": "https://wardheernews.com/",
        "rss": "https://wardheernews.com/feed/",
        "article_selector": "h3.entry-title a, .title a",
        "region": "Diaspora",
        "lang": "en",
        "email": "editor@wardheernews.com",
    },
    # ── Regional outlets ────────────────────────────────────────────────────
    {
        "name": "Puntland Post",
        "url": "https://puntlandpost.net/",
        "rss": "https://puntlandpost.net/feed/",
        "article_selector": "h2.entry-title a, h3 a",
        "region": "Puntland",
        "lang": "so+en",
        "email": "info@puntlandpost.net",
    },
    {
        "name": "Somaliland Sun",
        "url": "https://somalilandsun.com/",
        "rss": "https://somalilandsun.com/feed/",
        "article_selector": "h3.entry-title a, h2 a",
        "region": "Somaliland",
        "lang": "en",
        "email": "editor@somalilandsun.com",
    },
    {
        "name": "Haatuf (Somaliland)",
        "url": "https://haatuf.net/",
        "rss": "https://haatuf.net/feed/",
        "article_selector": "h3.entry-title a, .post-title a",
        "region": "Somaliland",
        "lang": "so",
        "email": "haatuf@haatuf.net",
    },
    {
        "name": "Horn Observer",
        "url": "https://hornobserver.com/",
        "rss": "https://hornobserver.com/feed/",
        "article_selector": "h3.entry-title a, h2 a",
        "region": "National",
        "lang": "en",
        "email": "info@hornobserver.com",
    },
    {
        "name": "Somali Current",
        "url": "https://somalicurrent.com/",
        "rss": "https://somalicurrent.com/feed/",
        "article_selector": "h2.entry-title a, h3 a",
        "region": "National",
        "lang": "en",
        "email": "info@somalicurrent.com",
    },
    {
        "name": "Radio Ergo",
        "url": "https://www.radioergo.org/en/",
        "rss": "https://www.radioergo.org/en/rss.xml",
        "article_selector": "h3 a, .article-title a",
        "region": "National",
        "lang": "so+en",
        "email": "info@radioergo.org",
    },
    {
        "name": "SONNA (Official)",
        "url": "https://sonna.so/en/",
        "rss": "https://sonna.so/en/feed/",
        "article_selector": "h2.entry-title a, h3 a",
        "region": "National",
        "lang": "en+so",
        "email": "info@sonna.so",
    },
    {
        "name": "Somali Memo",
        "url": "https://somalimemo.net/",
        "rss": "https://somalimemo.net/feed/",
        "article_selector": "h3.entry-title a, h2 a",
        "region": "Diaspora",
        "lang": "so",
        "email": "info@somalimemo.net",
    },
    {
        "name": "Awdal News",
        "url": "https://awdalnews.com/",
        "rss": "https://awdalnews.com/feed/",
        "article_selector": "h2.entry-title a, h3 a",
        "region": "Somaliland",
        "lang": "so+en",
        "email": "news@awdalnews.com",
    },
    {
        "name": "Jubbaland News",
        "url": "https://jubbalandnews.com/",
        "rss": "https://jubbalandnews.com/feed/",
        "article_selector": "h3.entry-title a, h2 a",
        "region": "Jubbaland",
        "lang": "so+en",
        "email": "info@jubbalandnews.com",
    },
]

# ─── CLASSIFICATION ─────────────────────────────────────────────────────────────
TOPIC_KEYWORDS = {
    "Security":     ["attack","bomb","explosion","militant","al-shabaab","shabaab","military","army","police","amisom","atmis","gunfire","killed","wounded","airstrike","troops","mortar","IED","insurgent","offensive"],
    "Governance":   ["parliament","president","prime minister","minister","government","election","vote","law","policy","federal","regional","senate","speaker","corruption","reform","constitution","FGS","FMS"],
    "Humanitarian": ["IDP","displacement","flood","drought","famine","food","aid","refugee","UNHCR","WFP","UNICEF","NGO","shelter","water","cholera","malnutrition","humanitarian","crisis"],
    "Economy":      ["economy","trade","port","import","export","SOS","shilling","dollar","inflation","investment","market","agriculture","livestock","fishing","khat","remittance","hawala"],
    "Health":       ["hospital","clinic","disease","outbreak","cholera","measles","polio","WHO","vaccination","health","COVID","malaria","maternal","death","epidemic"],
    "Elections":    ["election","vote","NIEC","ballot","candidate","campaign","political party","seat","constituency","result","registration","voter"],
    "Education":    ["school","university","student","teacher","education","UNICEF","USAID","classroom","literacy","curriculum","exam"],
}

SEVERITY_KEYWORDS = {
    "critical": ["killed","dead","explosion","bomb","attack","flood","famine","emergency","critical","crisis","mass casualty","airstrike","mass displacement"],
    "high":     ["arrested","protest","fighting","conflict","displaced","drought","outbreak","parliament","suspended","coalition","resign","reform","offensive"],
    "low":      []  # default
}

def classify(text):
    text_lower = text.lower()
    topic = "General"
    for t, kws in TOPIC_KEYWORDS.items():
        if any(k in text_lower for k in kws):
            topic = t
            break
    severity = "low"
    for s, kws in SEVERITY_KEYWORDS.items():
        if any(k in text_lower for k in kws):
            severity = s
            break
    return topic, severity

def detect_somali(text):
    somali_markers = ["ayaa","waxaa","si","xaaladda","shacabka","dowladda","magaalada","xukuumadda","madaxweynaha","baarlamaanka","ciidanka","codbixinta","doorashada","qorshaha"]
    return sum(1 for m in somali_markers if m in text.lower()) >= 2

# ─── TRANSLATION ───────────────────────────────────────────────────────────────
def translate_so_to_en(text):
    if not HF_API_KEY or not detect_somali(text):
        return text
    try:
        r = requests.post(
            "https://api-inference.huggingface.co/models/Helsinki-NLP/opus-mt-so-en",
            headers={"Authorization": f"Bearer {HF_API_KEY}"},
            json={"inputs": text[:500]},
            timeout=10
        )
        if r.ok:
            result = r.json()
            if isinstance(result, list) and result:
                return result[0].get("translation_text", text)
    except Exception as e:
        log.warning(f"Translation failed: {e}")
    return text

# ─── DATABASE ──────────────────────────────────────────────────────────────────
def get_db():
    conn = sqlite3.connect(DB_PATH)
    conn.row_factory = sqlite3.Row
    return conn

def init_db():
    conn = get_db()
    c = conn.cursor()
    c.executescript("""
        CREATE TABLE IF NOT EXISTS articles (
            id         INTEGER PRIMARY KEY AUTOINCREMENT,
            source     TEXT NOT NULL,
            region     TEXT DEFAULT 'National',
            url        TEXT UNIQUE NOT NULL,
            title_orig TEXT,
            title_en   TEXT,
            summary    TEXT,
            topic      TEXT DEFAULT 'General',
            severity   TEXT DEFAULT 'low',
            lang       TEXT DEFAULT 'en',
            scraped_at TEXT NOT NULL
        );
        CREATE TABLE IF NOT EXISTS clients (
            id         INTEGER PRIMARY KEY AUTOINCREMENT,
            org_name   TEXT NOT NULL,
            email      TEXT UNIQUE NOT NULL,
            plan       TEXT DEFAULT 'standard',
            keywords   TEXT DEFAULT '',
            topics     TEXT DEFAULT 'Security,Governance,Humanitarian',
            active     INTEGER DEFAULT 1,
            created_at TEXT NOT NULL
        );
        CREATE TABLE IF NOT EXISTS alerts_sent (
            id         INTEGER PRIMARY KEY AUTOINCREMENT,
            client_id  INTEGER,
            article_id INTEGER,
            sent_at    TEXT NOT NULL
        );
        CREATE TABLE IF NOT EXISTS api_keys (
            id         INTEGER PRIMARY KEY AUTOINCREMENT,
            client_id  INTEGER,
            key_hash   TEXT UNIQUE NOT NULL,
            created_at TEXT NOT NULL
        );
    """)
    conn.commit()
    conn.close()
    log.info("Database ready")

# ─── SCRAPING ──────────────────────────────────────────────────────────────────
HEADERS = {
    "User-Agent": "Mozilla/5.0 (compatible; MuuqaalBot/1.0; +https://muuqaal.so/bot)"
}

def scrape_rss(source):
    """Try RSS first — faster and more reliable than HTML scraping."""
    articles = []
    rss_url = source.get("rss")
    if not rss_url:
        return articles
    try:
        r = requests.get(rss_url, headers=HEADERS, timeout=12)
        if not r.ok:
            return articles
        soup = BeautifulSoup(r.text, "lxml-xml")
        items = soup.find_all("item")[:15]
        for item in items:
            title = item.find("title")
            link  = item.find("link")
            desc  = item.find("description")
            if not title or not link:
                continue
            title_text = title.get_text(strip=True)
            url_text   = link.get_text(strip=True)
            desc_text  = BeautifulSoup(desc.get_text(strip=True) if desc else "", "html.parser").get_text()[:300]
            articles.append({
                "source":     source["name"],
                "region":     source.get("region", "National"),
                "url":        url_text,
                "title_orig": title_text,
                "summary":    desc_text,
                "lang":       source.get("lang", "en"),
            })
    except Exception as e:
        log.warning(f"RSS failed for {source['name']}: {e}")
    return articles

def scrape_html(source):
    """Fallback: scrape HTML with BeautifulSoup."""
    articles = []
    try:
        r = requests.get(source["url"], headers=HEADERS, timeout=15)
        if not r.ok:
            return articles
        soup = BeautifulSoup(r.text, "lxml")
        links = soup.select(source["article_selector"])[:12]
        for a in links:
            title = a.get_text(strip=True)
            href  = a.get("href", "")
            if not title or not href or len(title) < 10:
                continue
            if not href.startswith("http"):
                from urllib.parse import urljoin
                href = urljoin(source["url"], href)
            articles.append({
                "source":     source["name"],
                "region":     source.get("region", "National"),
                "url":        href,
                "title_orig": title,
                "summary":    "",
                "lang":       source.get("lang", "en"),
            })
    except Exception as e:
        log.warning(f"HTML scrape failed for {source['name']}: {e}")
    return articles

def scrape_source(source):
    articles = scrape_rss(source)
    if not articles:
        articles = scrape_html(source)
    log.info(f"  {source['name']}: {len(articles)} articles")
    return articles

def scrape_all_sources():
    all_articles = []
    for source in SOURCES:
        try:
            articles = scrape_source(source)
            all_articles.extend(articles)
        except Exception as e:
            log.error(f"Source {source['name']} failed: {e}")
        time.sleep(0.5)  # polite crawl delay
    log.info(f"Total scraped: {len(all_articles)} articles from {len(SOURCES)} sources")
    return all_articles

def store_articles(raw_articles):
    conn = get_db()
    c = conn.cursor()
    new_count = 0
    for art in raw_articles:
        try:
            # Translate if Somali
            title_en = translate_so_to_en(art["title_orig"])
            text_for_classify = f"{title_en} {art.get('summary','')}"
            topic, severity = classify(text_for_classify)
            c.execute("""
                INSERT OR IGNORE INTO articles
                  (source, region, url, title_orig, title_en, summary, topic, severity, lang, scraped_at)
                VALUES (?,?,?,?,?,?,?,?,?,?)
            """, (
                art["source"], art["region"], art["url"],
                art["title_orig"], title_en, art.get("summary",""),
                topic, severity, art["lang"],
                datetime.now(timezone.utc).isoformat()
            ))
            if c.rowcount:
                new_count += 1
        except Exception as e:
            log.warning(f"Store error: {e}")
    conn.commit()
    conn.close()
    log.info(f"Stored {new_count} new articles")
    return new_count

# ─── EMAIL ─────────────────────────────────────────────────────────────────────
def send_email(to_email, subject, html_body):
    if not SMTP_USER or not SMTP_PASS:
        log.warning(f"SMTP not configured — would send to {to_email}: {subject}")
        return False
    try:
        msg = MIMEMultipart("alternative")
        msg["Subject"] = subject
        msg["From"]    = f"Muuqaal Monitor <{FROM_EMAIL}>"
        msg["To"]      = to_email
        msg.attach(MIMEText(html_body, "html"))
        with smtplib.SMTP(SMTP_HOST, SMTP_PORT) as s:
            s.starttls()
            s.login(SMTP_USER, SMTP_PASS)
            s.sendmail(FROM_EMAIL, to_email, msg.as_string())
        return True
    except Exception as e:
        log.error(f"Email to {to_email} failed: {e}")
        return False

def send_critical_alerts(new_article_ids):
    if not new_article_ids:
        return
    conn = get_db()
    placeholders = ",".join("?" * len(new_article_ids))
    articles = conn.execute(
        f"SELECT * FROM articles WHERE id IN ({placeholders}) AND severity='critical'",
        new_article_ids
    ).fetchall()
    clients = conn.execute("SELECT * FROM clients WHERE active=1").fetchall()
    for art in articles:
        for client in clients:
            already_sent = conn.execute(
                "SELECT 1 FROM alerts_sent WHERE client_id=? AND article_id=?",
                (client["id"], art["id"])
            ).fetchone()
            if already_sent:
                continue
            html = f"""
<div style="font-family:sans-serif;max-width:600px;margin:0 auto">
  <div style="background:#1e293b;padding:16px 24px;border-radius:8px 8px 0 0">
    <h2 style="color:#fff;margin:0">🔴 Critical Alert — Muuqaal Monitor</h2>
  </div>
  <div style="background:#f8fafc;padding:20px 24px;border:1px solid #e2e8f0">
    <p style="color:#64748b;font-size:13px">For: {client['org_name']} | {datetime.now().strftime('%d %b %Y %H:%M')} EAT</p>
    <h3 style="color:#1e293b">{art['title_en'] or art['title_orig']}</h3>
    <p style="color:#475569">{art.get('summary','')}</p>
    <p style="margin-top:12px"><span style="background:#fef2f2;color:#dc2626;padding:3px 10px;border-radius:4px;font-size:12px;font-weight:700">CRITICAL</span>
    &nbsp;<span style="background:#eff6ff;color:#2563eb;padding:3px 10px;border-radius:4px;font-size:12px">{art['topic']}</span>
    &nbsp;<span style="font-size:12px;color:#94a3b8">{art['source']} · {art['region']}</span></p>
    <p style="margin-top:16px"><a href="{art['url']}" style="color:#6366f1">Read full article →</a></p>
  </div>
  <div style="background:#f1f5f9;padding:12px 24px;font-size:11px;color:#94a3b8;border-radius:0 0 8px 8px">
    Muuqaal Monitor · muuqaal.so · alerts@muuqaal.so
  </div>
</div>"""
            if send_email(client["email"], f"🔴 Critical: {art['title_en'][:60]}", html):
                conn.execute(
                    "INSERT INTO alerts_sent (client_id,article_id,sent_at) VALUES (?,?,?)",
                    (client["id"], art["id"], datetime.now(timezone.utc).isoformat())
                )
                conn.commit()
    conn.close()

def send_daily_digest():
    conn = get_db()
    clients = conn.execute("SELECT * FROM clients WHERE active=1").fetchall()
    articles = conn.execute(
        "SELECT * FROM articles WHERE DATE(scraped_at)=DATE('now') ORDER BY severity DESC, id DESC LIMIT 30"
    ).fetchall()
    for client in clients:
        # Filter to client's topics
        client_topics = (client["topics"] or "Security,Governance,Humanitarian").split(",")
        client_kws    = [k.strip().lower() for k in (client["keywords"] or "").split(",") if k.strip()]
        relevant = [
            a for a in articles
            if a["topic"] in client_topics
            or any(kw in (a["title_en"] or "").lower() for kw in client_kws)
        ][:15]
        if not relevant:
            relevant = list(articles[:10])
        
        sev_colors = {"critical":"#dc2626","high":"#ea580c","low":"#64748b"}
        rows = ""
        for a in relevant:
            color = sev_colors.get(a["severity"], "#64748b")
            rows += f"""
<tr>
  <td style="padding:12px 0;border-bottom:1px solid #f1f5f9;vertical-align:top">
    <span style="background:{color};color:#fff;padding:2px 7px;border-radius:4px;font-size:10px;font-weight:700;text-transform:uppercase">{a['severity']}</span>
    <span style="background:#eff6ff;color:#2563eb;padding:2px 7px;border-radius:4px;font-size:10px;margin-left:4px">{a['topic']}</span>
    <span style="font-size:11px;color:#94a3b8;margin-left:6px">{a['source']} · {a['region']}</span><br>
    <a href="{a['url']}" style="color:#1e293b;font-weight:600;font-size:14px;text-decoration:none">{a['title_en'] or a['title_orig']}</a>
    <p style="color:#64748b;font-size:12px;margin:4px 0 0">{(a['summary'] or '')[:150]}...</p>
  </td>
</tr>"""
        
        html = f"""
<div style="font-family:sans-serif;max-width:640px;margin:0 auto">
  <div style="background:#1e293b;padding:20px 28px;border-radius:8px 8px 0 0">
    <h2 style="color:#fff;margin:0;font-size:20px">🇸🇴 Muuqaal Monitor — Daily Briefing</h2>
    <p style="color:#94a3b8;font-size:12px;margin:4px 0 0">{datetime.now().strftime('%A %d %B %Y')} · {client['org_name']} · {client['plan'].title()} plan</p>
  </div>
  <div style="background:#f8fafc;padding:20px 28px;border:1px solid #e2e8f0">
    <p style="color:#64748b;font-size:14px;margin:0 0 16px">Good morning. {len(relevant)} stories from Somali-language media matching your profile today.</p>
    <table width="100%" cellpadding="0" cellspacing="0">{rows}</table>
  </div>
  <div style="background:#f1f5f9;padding:14px 28px;font-size:11px;color:#94a3b8;border-radius:0 0 8px 8px;border:1px solid #e2e8f0;border-top:none">
    Muuqaal Monitor · muuqaal.so · alerts@muuqaal.so · Standard Plan ${700 if client['plan']=='standard' else 1800}/month
  </div>
</div>"""
        send_email(client["email"], f"🇸🇴 Muuqaal Daily Briefing — {datetime.now().strftime('%d %b %Y')}", html)
    conn.close()
    log.info("Daily digest sent")

# ─── PIPELINE ──────────────────────────────────────────────────────────────────
def run_pipeline():
    log.info("=== Pipeline start ===")
    raw = scrape_all_sources()
    conn = get_db()
    c = conn.cursor()
    new_ids = []
    for art in raw:
        title_en = translate_so_to_en(art["title_orig"])
        topic, severity = classify(f"{title_en} {art.get('summary','')}")
        c.execute("""
            INSERT OR IGNORE INTO articles
              (source,region,url,title_orig,title_en,summary,topic,severity,lang,scraped_at)
            VALUES (?,?,?,?,?,?,?,?,?,?)
        """, (
            art["source"], art["region"], art["url"],
            art["title_orig"], title_en, art.get("summary",""),
            topic, severity, art["lang"],
            datetime.now(timezone.utc).isoformat()
        ))
        if c.rowcount:
            new_ids.append(c.lastrowid)
    conn.commit()
    conn.close()
    log.info(f"Pipeline: {len(new_ids)} new articles")
    send_critical_alerts(new_ids)
    return len(new_ids)

# ─── AUTH ───────────────────────────────────────────────────────────────────────
def require_auth(f):
    @wraps(f)
    def decorated(*args, **kwargs):
        token = request.headers.get("X-API-Key", "")
        if token == ADMIN_TOKEN:
            return f(*args, **kwargs)
        token_hash = hashlib.sha256(token.encode()).hexdigest()
        conn = get_db()
        key = conn.execute("SELECT * FROM api_keys WHERE key_hash=?", (token_hash,)).fetchone()
        conn.close()
        if not key:
            return jsonify({"error": "Unauthorized"}), 401
        return f(*args, **kwargs)
    return decorated

# ─── API ROUTES ─────────────────────────────────────────────────────────────────

@app.route("/")
def index():
    return jsonify({
        "service": "Muuqaal Monitor",
        "version": "1.0",
        "sources": len(SOURCES),
        "endpoints": ["/api/feed", "/api/articles", "/api/clients", "/api/sources",
                      "/api/stats", "/api/pipeline/run", "/api/digest/send"]
    })

@app.route("/api/feed")
def api_feed():
    """Main endpoint consumed by the Muuqaal Monitor Dashboard HTML."""
    conn = get_db()
    limit = min(int(request.args.get("limit", 50)), 200)
    topic = request.args.get("topic")
    severity = request.args.get("severity")
    source = request.args.get("source")
    region = request.args.get("region")

    query = "SELECT * FROM articles WHERE 1=1"
    params = []
    if topic:    query += " AND topic=?";    params.append(topic)
    if severity: query += " AND severity=?"; params.append(severity)
    if source:   query += " AND source=?";   params.append(source)
    if region:   query += " AND region=?";   params.append(region)
    query += " ORDER BY scraped_at DESC LIMIT ?"
    params.append(limit)

    rows = conn.execute(query, params).fetchall()
    total = conn.execute("SELECT COUNT(*) FROM articles").fetchone()[0]
    today = conn.execute("SELECT COUNT(*) FROM articles WHERE DATE(scraped_at)=DATE('now')").fetchone()[0]
    critical = conn.execute("SELECT COUNT(*) FROM articles WHERE severity='critical' AND DATE(scraped_at)=DATE('now')").fetchone()[0]
    clients_count = conn.execute("SELECT COUNT(*) FROM clients WHERE active=1").fetchone()[0]
    conn.close()

    articles = []
    for r in rows:
        articles.append({
            "id":         r["id"],
            "source":     r["source"],
            "region":     r["region"],
            "url":        r["url"],
            "title":      r["title_en"] or r["title_orig"],
            "title_so":   r["title_orig"],
            "summary":    r["summary"],
            "topic":      r["topic"],
            "severity":   r["severity"],
            "lang":       r["lang"],
            "scraped_at": r["scraped_at"],
        })

    return jsonify({
        "articles": articles,
        "stats": {
            "total": total,
            "today": today,
            "critical_today": critical,
            "active_clients": clients_count,
            "sources": len(SOURCES),
        },
        "sources": [{"name": s["name"], "region": s["region"]} for s in SOURCES],
        "user": {
            "org": "Muuqaal Monitor",
            "plan": "admin",
        }
    })

@app.route("/api/articles")
@require_auth
def get_articles():
    conn = get_db()
    rows = conn.execute(
        "SELECT * FROM articles ORDER BY scraped_at DESC LIMIT 100"
    ).fetchall()
    conn.close()
    return jsonify([dict(r) for r in rows])

@app.route("/api/sources")
def get_sources():
    return jsonify([{
        "name":   s["name"],
        "url":    s["url"],
        "region": s["region"],
        "lang":   s["lang"],
    } for s in SOURCES])

@app.route("/api/stats")
def get_stats():
    conn = get_db()
    total   = conn.execute("SELECT COUNT(*) FROM articles").fetchone()[0]
    today   = conn.execute("SELECT COUNT(*) FROM articles WHERE DATE(scraped_at)=DATE('now')").fetchone()[0]
    crit    = conn.execute("SELECT COUNT(*) FROM articles WHERE severity='critical'").fetchone()[0]
    clients = conn.execute("SELECT COUNT(*) FROM clients WHERE active=1").fetchone()[0]
    by_topic = conn.execute(
        "SELECT topic, COUNT(*) as n FROM articles GROUP BY topic ORDER BY n DESC"
    ).fetchall()
    by_source = conn.execute(
        "SELECT source, COUNT(*) as n FROM articles GROUP BY source ORDER BY n DESC LIMIT 10"
    ).fetchall()
    conn.close()
    return jsonify({
        "total": total, "today": today, "critical": crit,
        "active_clients": clients, "sources": len(SOURCES),
        "by_topic":  [dict(r) for r in by_topic],
        "by_source": [dict(r) for r in by_source],
    })

@app.route("/api/clients", methods=["GET", "POST"])
@require_auth
def clients():
    conn = get_db()
    if request.method == "POST":
        d = request.json
        conn.execute(
            "INSERT INTO clients (org_name,email,plan,keywords,topics,active,created_at) VALUES (?,?,?,?,?,1,?)",
            (d["org_name"], d["email"], d.get("plan","standard"),
             d.get("keywords",""), d.get("topics","Security,Governance,Humanitarian"),
             datetime.now(timezone.utc).isoformat())
        )
        conn.commit()
        conn.close()
        return jsonify({"status": "created"}), 201
    rows = conn.execute("SELECT * FROM clients").fetchall()
    conn.close()
    return jsonify([dict(r) for r in rows])

@app.route("/api/pipeline/run", methods=["POST"])
@require_auth
def trigger_pipeline():
    count = run_pipeline()
    return jsonify({"status": "ok", "new_articles": count})

@app.route("/api/digest/send", methods=["POST"])
@require_auth
def trigger_digest():
    send_daily_digest()
    return jsonify({"status": "ok"})

# ─── SCHEDULER ──────────────────────────────────────────────────────────────────
def start_scheduler():
    schedule.every(30).minutes.do(run_pipeline)
    schedule.every().day.at("03:00").do(send_daily_digest)   # 6 AM EAT
    def _run():
        while True:
            schedule.run_pending()
            time.sleep(60)
    t = threading.Thread(target=_run, daemon=True)
    t.start()
    log.info("Scheduler started: pipeline every 30min, digest at 03:00 UTC")

# ─── STARTUP ────────────────────────────────────────────────────────────────────
if __name__ == "__main__":
    init_db()
    start_scheduler()
    log.info(f"Muuqaal Monitor starting — {len(SOURCES)} sources configured")
    log.info("Dashboard: open Muuqaal_Monitor_Dashboard_FIXED.html in your browser")
    port = int(os.getenv("PORT", 5001))
    app.run(host="0.0.0.0", port=port, debug=False)
