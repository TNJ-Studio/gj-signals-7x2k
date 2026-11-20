#!/usr/bin/env python3
"""Gem & Jewellery B2B Signals - fetch, filter, dedupe, build dashboard.
Run every 15 min via cron / GitHub Actions:  python signals.py
Needs only Python 3.9+ (standard library)."""
import sqlite3, json, re, time, urllib.request, urllib.parse
from concurrent.futures import ThreadPoolExecutor
from email.utils import parsedate_to_datetime
from datetime import datetime, timezone, timedelta
import xml.etree.ElementTree as ET, os
WATCH_RX = {}  # filled in main()
FEEDCACHE = "feeds.json"

def discover(domain):
    """Find RSS/Atom feeds on a site: <link rel=alternate> first, then common paths."""
    cache = json.load(open(FEEDCACHE)) if os.path.exists(FEEDCACHE) else {}
    c = cache.get(domain)
    if c and time.time() - c["t"] < 7 * 86400: return c["f"]
    found = []
    html = get("https://" + domain).decode("utf-8", "ignore")
    for m in re.finditer(r"<link[^>]+>", html, re.I):
        tag = m.group(0)
        if re.search(r"(rss|atom)\+xml", tag, re.I):
            h = re.search(r'href=["\']([^"\']+)', tag)
            if h: found.append(urllib.parse.urljoin("https://" + domain, h.group(1)))
    if not found:
        for p in ("/feed", "/rss", "/feed.xml", "/rss.xml", "/news/rss", "/blog/feed"):
            u = "https://" + domain + p
            if parse(get(u)): found.append(u); break
    cache[domain] = {"t": time.time(), "f": found[:3]}
    json.dump(cache, open(FEEDCACHE, "w"))
    return found[:3]

# ---------- CONFIG: edit freely ----------
TOPICS = [
 '"rough diamond" OR "diamond miner" OR "diamond mining" OR "diamond bourse" OR "diamond exchange"',
 '"lab-grown diamond" OR "natural diamond council" OR "Kimberley Process" OR "De Beers" OR Alrosa',
 '"gold refinery" OR "gold refiner" OR LBMA OR "bullion" OR "World Gold Council" OR "gold miner"',
 '"jewellery manufacturer" OR "jewelry manufacturer" OR "jewellery exporter" OR "jewelry industry"',
 '"jewellery retailer" OR "jewelry retailer" OR Signet OR Pandora OR Tanishq OR Chow Tai Fook',
 '"colored gemstone" OR "coloured gemstone" OR "gem trade" OR "gemstone mining" OR "gem fair"',
 '"jewellery association" OR "jewelry association" OR GJEPC OR CIBJO OR "RJC" OR "hallmarking"',
 '"jewellery collection" OR "jewelry collection" OR "jewellery trade show" OR "JCK" OR "IIJS"',
 '"platinum" OR "silver" OR "palladium" AND "jewellery demand" OR "jewelry demand"',
]
EDITIONS = [("en-US","US","US:en"),("en-GB","GB","GB:en"),("en-IN","IN","IN:en"),
 ("en","AE","AE:en"),("en","BE","BE:en"),("en-HK","HK","HK:en"),("en-SG","SG","SG:en"),
 ("en","ZA","ZA:en"),("en-AU","AU","AU:en"),("it","IT","IT:it"),("fr","FR","FR:fr"),
 ("de","DE","DE:de"),("es","ES","ES:es")]
# Add direct RSS/Atom feeds here (associations, exchanges, miners, trade media). Verify each URL.
FEEDS = [
 # ("Source name", "https://example.com/feed"),
]
INCLUDE = re.compile(r"diamond|gem|jewel|gold|bullion|refiner|platinum|palladium|silver|bourse|hallmark|"
    r"lbma|kimberley|cibjo|gjepc|signet|pandora|de beers|alrosa|rapaport|tanishq|lab-grown|moissanite|pearl|"
    r"gioiell|bijou|schmuck|joyer|diamanti|oreficeria|เครื่องประดับ|เพชร|อัญมณี|mücevher|kuyumcu|elmas|"
    r"ювелир|алмаз|бриллиант|trang sức|kim cương|יהלום|תכשיט|مجوهر|ألماس|珠宝|珠寶|钻石|鑽石", re.I)
EXCLUDE = re.compile(r"theft|robbery|robbed|burglar|stolen|heist|looted|snatch|arrested|murder|police|"
    r"wedding|engagement ring|celebrity|red carpet|gold rate today|gold price today|horoscope|astrolog|"
    r"how to (buy|clean|wear)|best .* (rings|necklaces)|gift guide|black friday|fashion week|"
    r"outfit|royal family|kate middleton|bride|giveaway|obituary|"
    r"giá vàng|ราคาทอง|黄金价格|金价|金價|أسعار الذهب|سعر الذهب|prezzo (dell.)?oro|altın fiyat|курс золота|цена золота", re.I)
# Companies / bodies to always catch. name -> search phrase(s). Add more anytime.
WATCHLIST = {
 "GJEPC": '"GJEPC" OR "Gem & Jewellery Export Promotion Council"',
 "De Beers": '"De Beers"',
 "LBMA": '"LBMA" OR "London Bullion Market Association"',
 "World Gold Council": '"World Gold Council"',
 "The Diamond Collective": '"The Diamond Collective"',
 "Damiani": '"Damiani" jewellery',
 "DMCC": '"DMCC" OR "Dubai Multi Commodities Centre"',
 "Qatar Diamond Exchange": '"Qatar Diamond Exchange" OR "Qatar Diamond Bourse"',
 "Titan / Tanishq": '"Tanishq" OR "Titan Company" jewellery',
 "Mia by Tanishq": '"Mia by Tanishq"',
 "Malabar": '"Malabar Gold" OR "Malabar Gold & Diamonds"',
 "Le Pizzo": '"Le Pizzo" jewelry',
 "Smiling Rocks": '"Smiling Rocks" diamonds',
 "HKTDC": '"HKTDC" jewellery OR "Hong Kong Jewellery"',
 "Informa Markets": '"Informa Markets" jewellery OR jewelry OR gem',
 "Okavango": '"Okavango Diamond Company"',
 "Ahmed Bin Sulayem": '"Ahmed Bin Sulayem"',
 "Italian Exhibition Group": '"Italian Exhibition Group" OR "Vicenzaoro" OR "OroArezzo"',
}
# Countries you cover. name -> aliases. Searched worldwide so local bias never creeps in.
COUNTRIES = {
 "USA": '"United States" OR USA OR "U.S."', "India": 'India OR Mumbai OR Surat OR Jaipur',
 "China": 'China OR Shanghai OR Shenzhen', "Hong Kong": '"Hong Kong"', "Sri Lanka": '"Sri Lanka"',
 "Israel": 'Israel OR "Ramat Gan"', "Thailand": 'Thailand OR Bangkok OR Chanthaburi',
 "Italy": 'Italy OR Vicenza OR Valenza OR Arezzo', "UK": '"United Kingdom" OR UK OR London OR Birmingham',
 "Middle East": 'Dubai OR UAE OR Qatar OR Kuwait OR Bahrain OR Oman', "Saudi Arabia": '"Saudi Arabia" OR Riyadh',
 "Russia": 'Russia OR Alrosa', "Singapore": 'Singapore', "Vietnam": 'Vietnam OR "Ho Chi Minh"',
 "Australia": 'Australia', "New Zealand": '"New Zealand"', "South Africa": '"South Africa"',
 "Angola": 'Angola', "Botswana": 'Botswana OR Debswana', "Ghana": 'Ghana', "Turkey": 'Turkey OR Istanbul OR Türkiye',
}
GENERIC = '(diamond OR jewellery OR jewelry OR gemstone OR bullion OR "gold refinery" OR "gold mining" OR "gold miner")'
# Local-language sweeps: (hl, gl, ceid, country label, query or None for the generic English one)
LOCAL = [
 ("it","IT","IT:it","Italy","gioielli OR gioielleria OR diamanti OR oreficeria"),
 ("th","TH","TH:th","Thailand","เครื่องประดับ OR เพชร OR อัญมณี"),
 ("tr","TR","TR:tr","Turkey","mücevher OR kuyumcu OR elmas"),
 ("ru","RU","RU:ru","Russia","ювелирный OR алмазы OR бриллианты"),
 ("vi","VN","VN:vi","Vietnam","trang sức OR kim cương"),
 ("iw","IL","IL:he","Israel","יהלומים OR תכשיטים"),
 ("ar","SA","SA:ar","Saudi Arabia","مجوهرات OR ألماس"),
 ("ar","AE","AE:ar","Middle East","مجوهرات OR ألماس"),
 ("zh-HK","HK","HK:zh-Hant","Hong Kong","珠寶 OR 鑽石"),
 ("zh-CN","CN","CN:zh-Hans","China","珠宝 OR 钻石"),
 ("en","IL","IL:en","Israel",None),("en","SA","SA:en","Saudi Arabia",None),
 ("en","NZ","NZ:en","New Zealand",None),("en","GH","GH:en","Ghana",None),
 ("en","TH","TH:en","Thailand",None),("en","LK","LK:en","Sri Lanka",None),
 ("en","ZA","ZA:en","South Africa",None),("en","BW","BW:en","Botswana",None),
]
# Websites to monitor. (name, domain, mode)  mode: "all" = every story is relevant,
# "topic" = only stories matching the industry keywords (for general news outlets).
SITES = [
 ("GJEPC","gjepc.org","all"),("De Beers","debeersgroup.com","all"),("LBMA","lbma.org.uk","all"),
 ("World Gold Council","gold.org","all"),("Damiani","damiani.com","all"),("DMCC","dmcc.ae","topic"),
 ("HKTDC","hktdc.com","topic"),("Informa Markets","informamarkets.com","topic"),
 ("Okavango","okavangodiamonds.com","all"),("Titan","titancompany.in","topic"),
 ("Reuters","reuters.com","topic"),("Bloomberg","bloomberg.com","topic"),
 ("ANI","aninews.in","topic"),("Mining.com","mining.com","topic"),
 ("Italian Exhibition Group","iegexpo.it","topic"),("Khaleej Times","khaleejtimes.com","topic"),
 # Trade media / competitors (every story is relevant)
 ("Professional Jeweller","professionaljeweller.com","all"),("Instore","instoremag.com","all"),
 ("National Jeweler","nationaljeweler.com","all"),("Retail Jeweller India","retailjewellerindia.com","all"),
 ("Retail Jeweller UK","retail-jeweller.com","all"),("The Jewelry Wire","thejewelrywire.com","all"),
 ("Diamond Press","diamondpress.com","all"),
]
MAX_AGE_DAYS = 14
DB, OUT = "signals.db", "index.html"
# -----------------------------------------

UA = {"User-Agent": "Mozilla/5.0 (compatible; GemSignals/1.0)"}

def get(url):
    try:
        with urllib.request.urlopen(urllib.request.Request(url, headers=UA), timeout=25) as r:
            return r.read()
    except Exception as e:
        print("fail", url[:90], e); return b""

def parse(xml, default_src=""):
    out = []
    try: root = ET.fromstring(xml)
    except Exception: return out
    for it in root.iter("item"):
        t = (it.findtext("title") or "").strip(); l = (it.findtext("link") or "").strip()
        d = it.findtext("pubDate"); s = it.find("source")
        src = (s.text if s is not None and s.text else default_src)
        out.append((t, l, d, src))
    ns = "{http://www.w3.org/2005/Atom}"
    for e in root.iter(ns + "entry"):
        t = (e.findtext(ns+"title") or "").strip(); a = e.find(ns+"link")
        l = a.get("href") if a is not None else ""
        out.append((t, l, e.findtext(ns+"published") or e.findtext(ns+"updated"), default_src))
    return out

def when(d):
    try:
        dt = parsedate_to_datetime(d)
    except Exception:
        try: dt = datetime.fromisoformat((d or "").replace("Z", "+00:00"))
        except Exception: return None
    return dt if dt.tzinfo else dt.replace(tzinfo=timezone.utc)

def jobs():
    for q in TOPICS:
        for hl, gl, ceid in EDITIONS[:4]:   # country sweeps below cover the rest
            u = "https://news.google.com/rss/search?" + urllib.parse.urlencode(
                {"q": q + " when:2d", "hl": hl, "gl": gl, "ceid": ceid})
            yield u, "", gl, ""
    for c, alias in COUNTRIES.items():
        for hl, gl, ceid in (EDITIONS[0], EDITIONS[1]):   # US + UK editions = no local bias
            u = "https://news.google.com/rss/search?" + urllib.parse.urlencode(
                {"q": "(" + alias + ") " + GENERIC + " when:2d", "hl": hl, "gl": gl, "ceid": ceid})
            yield u, "", c, ""
    for hl, gl, ceid, c, q in LOCAL:
        u = "https://news.google.com/rss/search?" + urllib.parse.urlencode(
            {"q": (q or GENERIC) + " when:2d", "hl": hl, "gl": gl, "ceid": ceid})
        yield u, "", c, ""
    for name, u in FEEDS: yield u, name, "feed", name
    for name, q in WATCHLIST.items():
        for hl, gl, ceid in EDITIONS[:3]:   # US, GB, IN editions
            u = "https://news.google.com/rss/search?" + urllib.parse.urlencode(
                {"q": "(" + q + ") when:7d", "hl": hl, "gl": gl, "ceid": ceid})
            yield u, "", gl, name
    for name, dom, mode in SITES:
        for f in discover(dom): yield f, name, "feed", name if mode == "all" else ""
        q = "site:" + dom + (" when:7d" if mode == "all" else " (diamond OR jewellery OR jewelry OR gemstone OR bullion OR \"gold refinery\" OR \"gold miner\") when:7d")
        u = "https://news.google.com/rss/search?" + urllib.parse.urlencode(
            {"q": q, "hl": "en-US", "gl": "US", "ceid": "US:en"})
        yield u, name, "site", name if mode == "all" else ""

def run(j):
    time.sleep(0.2)
    return [(*x, j[2], j[3]) for x in parse(get(j[0]), j[1])]

def norm(t): return re.sub(r"[^a-z0-9]+", " ", t.lower()).strip()[:90]

def main():
    db = sqlite3.connect(DB)
    db.execute("""CREATE TABLE IF NOT EXISTS s(key TEXT PRIMARY KEY, title TEXT, link TEXT,
                  source TEXT, pub TEXT, region TEXT, seen TEXT, w TEXT DEFAULT '')""")
    try: db.execute("ALTER TABLE s ADD COLUMN w TEXT DEFAULT ''")
    except sqlite3.OperationalError: pass
    WATCH_RX.update({n: re.compile("|".join(re.findall(r'"([^"]+)"', q)) or re.escape(n), re.I)
                     for n, q in WATCHLIST.items()})
    now = datetime.now(timezone.utc)
    with ThreadPoolExecutor(6) as ex:
        results = [x for batch in ex.map(run, list(jobs())) for x in batch]
    results.sort(key=lambda x: len(x[4]) <= 2)   # country-labelled hits win the region tag
    new = 0
    for t, l, d, src, reg, tag in results:
        dt = when(d)
        if not t or not l or not dt or now - dt > timedelta(days=MAX_AGE_DAYS): continue
        title = re.sub(r"\s+-\s+" + re.escape(src) + r"$", "", t) if src else t
        hits = {n for n, rx in WATCH_RX.items() if rx.search(title)}
        if tag and (tag in WATCH_RX or tag in [x[0] for x in SITES]): hits.add(tag)
        if EXCLUDE.search(title) or not (hits or INCLUDE.search(title)): continue
        key, w = norm(title), ", ".join(sorted(hits))
        old = db.execute("SELECT w FROM s WHERE key=?", (key,)).fetchone()
        if old:
            if w and w != old[0]: db.execute("UPDATE s SET w=? WHERE key=?", (w, key))
            continue
        db.execute("INSERT INTO s VALUES(?,?,?,?,?,?,?,?)",
                   (key, title, l, src or "Unknown", dt.isoformat(), reg, now.isoformat(), w)); new += 1
    db.commit()
    cutoff = (now - timedelta(days=MAX_AGE_DAYS)).isoformat()
    rows = db.execute("SELECT key,title,link,source,pub,region,w FROM s WHERE pub>? ORDER BY pub DESC LIMIT 3000",
                      (cutoff,)).fetchall()
    data = [dict(id=r[0], t=r[1], u=r[2], s=r[3], p=r[4], r=r[5], w=r[6]) for r in rows]
    page = TEMPLATE.replace("__DATA__", json.dumps(data)).replace("__UPDATED__", now.isoformat())
    open(OUT, "w", encoding="utf-8").write(page)
    print(f"{new} new, {len(data)} total -> {OUT}")

TEMPLATE = r"""<!doctype html><html lang="en"><head><meta charset="utf-8">
<meta name="viewport" content="width=device-width,initial-scale=1"><meta http-equiv="refresh" content="300">
<title>Gem &amp; Jewellery B2B Signals</title><style>
:root{--bg:#fafaf7;--card:#fff;--tx:#1c1c1a;--mu:#6b6b66;--ac:#8a5a00;--bd:#e4e2da}
@media(prefers-color-scheme:dark){:root{--bg:#161614;--card:#1f1f1c;--tx:#ecebe5;--mu:#9a9990;--ac:#e0b050;--bd:#33332e}}
body{margin:0;background:var(--bg);color:var(--tx);font:15px/1.4 system-ui,sans-serif}
header{position:sticky;top:0;background:var(--bg);border-bottom:1px solid var(--bd);padding:12px 16px;z-index:5}
h1{font-size:18px;margin:0 0 8px}h1 small{color:var(--mu);font-weight:400;font-size:12px;margin-left:8px}
.bar{display:flex;gap:8px;flex-wrap:wrap}
button,input,select{font:inherit;padding:6px 10px;border:1px solid var(--bd);border-radius:6px;background:var(--card);color:var(--tx)}
button.on{background:var(--ac);color:#fff;border-color:var(--ac)}
main{max-width:900px;margin:0 auto;padding:12px 16px 60px}
.c{background:var(--card);border:1px solid var(--bd);border-radius:8px;padding:10px 12px;margin:8px 0}
.c a{color:var(--tx);font-weight:600;text-decoration:none}.c a:hover{color:var(--ac)}
.m{color:var(--mu);font-size:12px;margin-top:4px;display:flex;gap:10px;flex-wrap:wrap;align-items:center}
.m b{color:var(--ac)}.c.sourced{border-left:4px solid #2e8b57}.c.skip{opacity:.45}
.m button{padding:1px 8px;font-size:12px}
</style></head><body>
<header><h1>Gem &amp; Jewellery B2B Signals<small id="up"></small></h1>
<div class="bar"><span id="w"></span>
<input id="q" placeholder="Search…"><select id="src"><option value="">All sources</option></select>
<select id="rg"><option value="">All regions</option></select><select id="st"><option value="">All</option><option value="new">Unreviewed</option>
<option value="sourced">Sourced</option><option value="skip">Skipped</option></select>
<button id="wl">★ Watchlist</button><span id="n" style="align-self:center;color:var(--mu)"></span></div></header><main id="list"></main>
<script>
const D=__DATA__,UP=new Date("__UPDATED__");let H=4,WL=false;
let S={};try{S=JSON.parse(localStorage.getItem("st")||"{}")}catch(e){}
const save=()=>{try{localStorage.setItem("st",JSON.stringify(S))}catch(e){}};
const $=i=>document.getElementById(i);
$("up").textContent="updated "+UP.toLocaleString();
[[4,"4h"],[12,"12h"],[24,"24h"],[168,"7d"],[9999,"All"]].forEach(([h,l])=>{
 const b=document.createElement("button");b.textContent=l;b.dataset.h=h;if(h==4)b.className="on";
 b.onclick=()=>{H=h;document.querySelectorAll("#w button").forEach(x=>x.className=x.dataset.h==h?"on":"");render()};$("w").append(b)});
[...new Set(D.map(d=>d.r))].sort().forEach(s=>{const o=document.createElement("option");o.value=o.textContent=s;$("rg").append(o)});
[...new Set(D.map(d=>d.s))].sort().forEach(s=>{const o=document.createElement("option");o.value=o.textContent=s;$("src").append(o)});
const ago=m=>m<60?m+"m ago":m<1440?Math.floor(m/60)+"h ago":Math.floor(m/1440)+"d ago";
const esc=s=>s.replace(/[&<>"]/g,c=>({"&":"&amp;","<":"&lt;",">":"&gt;",'"':"&quot;"}[c]));
function mark(id,v){S[id]=S[id]===v?"":v;save();render()}window.mark=mark;
function render(){const q=$("q").value.toLowerCase(),s=$("src").value,st=$("st").value,now=Date.now();
 const r=D.filter(d=>(now-new Date(d.p))/36e5<=H&&(!s||d.s==s)&&(!WL||d.w)&&(!$("rg").value||d.r==$("rg").value)&&(!q||(d.t+d.s).toLowerCase().includes(q))
  &&(!st||(st=="new"?!S[d.id]:S[d.id]==st)));
 $("wl").className=WL?"on":"";$("n").textContent=r.length+" signals";
 $("list").innerHTML=r.map(d=>{const m=Math.max(0,Math.round((now-new Date(d.p))/6e4)),k=S[d.id]||"";
  return `<div class="c ${k}"><a href="${esc(d.u)}" target="_blank" rel="noopener">${esc(d.t)}</a>
  <div class="m"><b>${esc(d.s)}</b><span>${esc(d.r)}</span><span>${ago(m)}</span>${d.w?`<b>★ ${esc(d.w)}</b>`:""}
  <button onclick="mark('${d.id.replace(/'/g,"")}','sourced')">${k=="sourced"?"✓ Sourced":"Sourced"}</button>
  <button onclick="mark('${d.id.replace(/'/g,"")}','skip')">${k=="skip"?"Skipped":"Skip"}</button></div></div>`}).join("")
  ||'<p style="color:var(--mu)">Nothing in this window. Try a longer one.</p>'}
$("wl").onclick=()=>{WL=!WL;render()};["q","src","st","rg"].forEach(i=>$(i).oninput=render);render();
</script></body></html>"""

if __name__ == "__main__":
    main()
