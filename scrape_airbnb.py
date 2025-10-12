from playwright.sync_api import sync_playwright
import csv, re, time, random, urllib.parse, os

# =============== CONFIG ===============
# COLLE ICI TON URL Airbnb (recherche avec filtres), SANS items_offset/section_offset.
SEARCH_URL_BASE = "COLLE_ICI_TON_URL_AIRBNB"

# Combien de NOUVELLES annonces max par run (lot). Modifie à 100, 150, etc.
MAX_NEW_LISTINGS_PER_RUN = 100

# Garde-fous / pagination
ITEMS_PER_PAGE = 20
MAX_OFFSET = 4000                 # offset maxi tenté (sécurité)
SECTION_OFFSETS = [0,1,2,3,4,5]   # Airbnb varie parfois ce paramètre

# Fichiers de sortie
OUTPUT_RUN = "airbnb_listings_run.csv"       # nouvelles lignes de CE run
OUTPUT_MASTER = "airbnb_listings_master.csv" # cumul historique (anti-doublons)
# ======================================

RE_LICENSE_PRIMARY = re.compile(r"\b([A-Z]{3}-[A-Z]{3}-[A-Z0-9]{4,6})\b", re.IGNORECASE)
RE_LICENSE_FALLBACK = re.compile(
    r"(?:Registration(?:\s*No\.|\s*Number)?|Permit|License|Licence|DTCM)[^\n\r]*?([A-Z0-9][A-Z0-9\-\/]{3,40})",
    re.IGNORECASE,
)
RE_HOST_RATING = re.compile(r"([0-5]\.\d{1,2})\s*(?:out of 5|·|/5|rating|reviews)", re.IGNORECASE)

def pause(a=0.7, b=1.2): time.sleep(random.uniform(a,b))
def clean(s): return (s or "").replace("\xa0"," ").strip()

def build_page_url(base, items_offset, section_offset):
    parsed = urllib.parse.urlparse(base)
    q = dict(urllib.parse.parse_qsl(parsed.query, keep_blank_values=True))
    q["items_offset"] = str(items_offset)
    q["section_offset"] = str(section_offset)
    new_q = urllib.parse.urlencode(q)
    return urllib.parse.urlunparse(parsed._replace(query=new_q))

def load_seen_urls(master_path):
    seen = set()
    if os.path.exists(master_path):
        with open(master_path, "r", encoding="utf-8-sig", newline="") as f:
            reader = csv.DictReader(f)
            for row in reader:
                u = (row.get("url_annonce") or "").strip()
                if u: seen.add(u)
    return seen

def extract_license(page):
    try:
        c = page.query_selector("div[data-testid='listing-permit-license-number']")
        if c:
            spans = c.query_selector_all("span")
            if spans and len(spans)>=2:
                val = clean(spans[-1].inner_text())
                m = RE_LICENSE_PRIMARY.search(val) or RE_LICENSE_FALLBACK.search(val)
                return (m.group(1) if m else val).upper()
    except: pass
    for sel in [
        "div:has-text('Permit number')","div:has-text('Dubai Tourism permit number')",
        "div:has-text('Registration')","div:has-text('License')","div:has-text('Licence')",
        "div:has-text('DTCM')","section[aria-labelledby*='About this space']","div[data-section-id='DESCRIPTION_DEFAULT']",
    ]:
        try:
            el = page.query_selector(sel)
            if el:
                txt = clean(el.inner_text())
                m = RE_LICENSE_PRIMARY.search(txt) or RE_LICENSE_FALLBACK.search(txt)
                if m: return m.group(1).upper()
        except: pass
    try:
        body = clean(page.inner_text("body"))
        m = RE_LICENSE_PRIMARY.search(body) or RE_LICENSE_FALLBACK.search(body)
        if m: return m.group(1).upper()
    except: pass
    return ""

def scrape_host_profile(context, host_url):
    note, nb_listings, joined = "", "", ""
    p = context.new_page()
    p.goto(host_url, timeout=90000)
    # forcer les chargements paresseux
    for _ in range(12):
        p.evaluate("window.scrollBy(0, document.body.scrollHeight)")
        pause(0.25, 0.45)
    try:
        body = p.inner_text("body")
        m = RE_HOST_RATING.search(body)
        if m: note = m.group(1)
    except: pass
    try:
        cards = p.query_selector_all("a[href*='/rooms/']")
        nb_listings = str(len({(c.get_attribute('href') or '').split('?')[0] for c in cards}))
    except: pass
    try:
        j = p.query_selector("span:has-text('Joined')") or p.query_selector("div:has-text('Joined')")
        if j: joined = clean(j.inner_text())
    except: pass
    p.close()
    return note, nb_listings, joined

def collect_urls_from_page(page, exclude_set):
    tmp = []
    try:
        page.wait_for_selector("a[href*='/rooms/']", timeout=30000)
        for a in page.query_selector_all("a[href*='/rooms/']"):
            href = (a.get_attribute("href") or "").strip()
            if not href: continue
            if href.startswith("/"): href = "https://www.airbnb.com"+href
            href = href.split("?")[0]
            if "/rooms/" in href and href not in exclude_set:
                tmp.append(href)
    except: pass
    # Dédup locale (ordre conservé)
    seen, out = set(), []
    for u in tmp:
        if u not in seen:
            seen.add(u); out.append(u)
    return out

def main():
    if "airbnb." not in SEARCH_URL_BASE:
        raise SystemExit("Mets ton URL Airbnb dans SEARCH_URL_BASE.")

    # Charger URLs déjà scrappées pour anti-doublons inter-runs
    seen_global = load_seen_urls(OUTPUT_MASTER)
    print(f"URLs déjà présentes (master): {len(seen_global)}")

    with sync_playwright() as pw:
        browser = pw.chromium.launch(headless=True)
        context = browser.new_context(
            user_agent="Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/115 Safari/537.36",
            locale="en-US",
        )
        # Accélérer : bloquer images / médias / fonts
        context.route("**/*", lambda r: r.abort() if r.request.resource_type in ["image","media","font"] else r.continue_())
        page = context.new_page()

        new_urls = []
        items_offset = 0

        # Pagination par offset jusqu’à atteindre le lot (100) de nouvelles annonces
        while items_offset <= MAX_OFFSET and len(new_urls) < MAX_NEW_LISTINGS_PER_RUN:
            found_on_this_offset = False
            for so in SECTION_OFFSETS:
                url = build_page_url(SEARCH_URL_BASE, items_offset, so)
                print(f"Page offset={items_offset} section_offset={so}")
                try:
                    page.goto(url, timeout=120000); pause()
                    urls = collect_urls_from_page(page, exclude_set=seen_global)
                    if urls:
                        # ajout seulement des nouvelles URL par rapport AU MASTER
                        for u in urls:
                            if len(new_urls) >= MAX_NEW_LISTINGS_PER_RUN:
                                break
                            if u not in seen_global:
                                new_urls.append(u)
                                seen_global.add(u)  # pour éviter de les reprendre à l’offset suivant
                        print(f"  +{len(urls)} candidates, total nouvelles={len(new_urls)}")
                        found_on_this_offset = True
                        break  # on passe au prochain offset
                except Exception as e:
                    print("  (skip) erreur:", e)
            items_offset += ITEMS_PER_PAGE
            if not found_on_this_offset:
                print("  (info) rien de nouveau ici, on avance…")

        print(f"Total d’URLs NOUVELLES à visiter dans ce run: {len(new_urls)}")

        # Scrape détaillé des annonces nouvelles
        rows = []
        for i, url in enumerate(new_urls, 1):
            try:
                page.goto(url, timeout=120000); pause()
                # cookies
                try:
                    btn = page.query_selector("button:has-text('Accept')") or page.query_selector("button:has-text('OK')")
                    if btn: btn.click()
                except: pass
                # titre
                titre = ""
                try:
                    h1 = page.query_selector("h1"); titre = clean(h1.inner_text() if h1 else "")
                except: pass
                # licence
                code = extract_license(page)
                # hôte
                nom_hote, host_url = "", ""
                try:
                    hl = page.query_selector("a[href*='/users/show/']")
                    if hl:
                        host_url = hl.get_attribute("href") or ""
                        if host_url.startswith("/"): host_url = "https://www.airbnb.com"+host_url
                        nom_hote = clean(hl.inner_text() or "")
                except: pass
                # profil hôte
                note, nb, joined = "", "", ""
                if host_url:
                    note, nb, joined = scrape_host_profile(context, host_url)

                rows.append({
                    "url_annonce": url,
                    "titre_annonce": titre,
                    "code_licence": code,
                    "nom_hote": nom_hote,
                    "url_profil_hote": host_url,
                    "note_globale_hote": note,
                    "nb_annonces_hote": nb,
                    "date_inscription_hote": joined,
                })
                print(f"[{i}/{len(new_urls)}] OK — {titre} — {code} — note:{note}")
            except Exception as e:
                print(f"[{i}] ERREUR {url}: {e}")

        # Écrire le CSV du RUN
        if rows:
            keys = list(rows[0].keys())
            with open(OUTPUT_RUN, "w", newline="", encoding="utf-8-sig") as f:
                w = csv.DictWriter(f, fieldnames=keys)
                w.writeheader()
                for r in rows: w.writerow(r)
            print("CSV du run écrit:", OUTPUT_RUN)
        else:
            print("Aucune donnée collectée pour ce run.")

        # Mettre à jour / créer le MASTER (anti-doublons)
        master_rows = []
        if os.path.exists(OUTPUT_MASTER):
            with open(OUTPUT_MASTER, "r", encoding="utf-8-sig", newline="") as f:
                reader = csv.DictReader(f)
                for row in reader:
                    master_rows.append(row)

        # fusionner (en évitant doublons par url_annonce)
        existing = { (r.get("url_annonce") or "").strip(): True for r in master_rows }
        for r in rows:
            u = (r.get("url_annonce") or "").strip()
            if u and u not in existing:
                master_rows.append(r)
                existing[u] = True

        if master_rows:
            keys = ["url_annonce","titre_annonce","code_licence","nom_hote","url_profil_hote","note_globale_hote","nb_annonces_hote","date_inscription_hote"]
            with open(OUTPUT_MASTER, "w", newline="", encoding="utf-8-sig") as f:
                w = csv.DictWriter(f, fieldnames=keys)
                w.writeheader()
                for r in master_rows: w.writerow(r)
            print("CSV maître mis à jour:", OUTPUT_MASTER)

        context.close(); browser.close()

if __name__ == "__main__":
    main()
