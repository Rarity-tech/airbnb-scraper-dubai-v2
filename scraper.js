import fs from "fs";
import path from "path";
import { fileURLToPath } from "url";
import Papa from "papaparse";
import puppeteer from "puppeteer";

const __filename = fileURLToPath(import.meta.url);
const __dirname = path.dirname(__filename);

const INPUT_FILE = path.join(__dirname, "urls.txt");
const OUT_DIR = path.join(__dirname, "output");
const OUT_CSV = path.join(OUT_DIR, "results.csv");
const NOW_YEAR = new Date().getFullYear();
const delay = (ms) => new Promise(r => setTimeout(r, ms));

function readUrls() {
  if (!fs.existsSync(INPUT_FILE)) throw new Error(`Fichier urls.txt introuvable: ${INPUT_FILE}`);

  let lines = fs.readFileSync(INPUT_FILE, "utf8")
    .split(/\r?\n/)
    .map(s => s.trim())
    .filter(s => s && !s.startsWith("#"));

  lines = lines.map(s => s.split(/[,;\t]/)[0].trim());
  const urls = lines.filter(s => /^https?:\/\//i.test(s));

  if (!urls.length) throw new Error("Aucune URL valide dans urls.txt");
  console.log(`✅ ${urls.length} URL(s) à scraper`);
  return urls;
}

function ensureOutDir() {
  if (!fs.existsSync(OUT_DIR)) fs.mkdirSync(OUT_DIR);
  const dbg = path.join(OUT_DIR, "debug");
  if (!fs.existsSync(dbg)) fs.mkdirSync(dbg);
  return dbg;
}

function yearFromAny(val) {
  if (val == null) return null;
  if (typeof val === "number") {
    const d = new Date(val > 1e12 ? val : val * 1000);
    const y = d.getUTCFullYear();
    if (y >= 2007 && y <= NOW_YEAR) return y;
  }
  if (typeof val === "string") {
    const m = val.match(/(19|20)\d{2}/);
    if (m) {
      const y = parseInt(m[0], 10);
      if (y >= 2007 && y <= NOW_YEAR) return y;
    }
  }
  return null;
}

function extractJoinedYearFromTextOrHtml(text, html) {
  const pats = [
    /Membre\s+depuis\s+(?:[A-Za-zÀ-ÖØ-öø-ÿ]+\s+)?(\d{4})/i,
    /Depuis\s+(?:[A-Za-zÀ-ÖØ-öø-ÿ]+\s+)?(\d{4})/i,
    /Inscrit[ e]*\s+(?:en|depuis)\s+(?:[A-Za-zÀ-ÖØ-öø-ÿ]+\s+)?(\d{4})/i,
    /Joined\s+in\s+(?:[A-Za-z]+\s+)?(\d{4})/i,
    /Member\s+since\s+(?:[A-Za-z]+\s+)?(\d{4})/i,
    /On\s+Airbnb\s+since\s+(?:[A-Za-z]+\s+)?(\d{4})/i,
    /Sur\s+Airbnb\s+depuis\s+(?:[A-Za-z]+\s+)?(\d{4})/i
  ];
  for (const re of pats) {
    let m = text.match(re);
    if (!m) m = html.match(re);
    if (m) {
      const y = parseInt(m[1], 10);
      if (y >= 2007 && y <= NOW_YEAR) return y;
    }
  }
  return null;
}

function extractListingCount(text, html) {
  const all = [];
  for (const re of [/(\d{1,4})\s+(annonces|hébergements|logements)/ig, /(\d{1,4})\s+listings?/ig]) {
    for (const m of (text.matchAll(re))) all.push(parseInt(m[1], 10));
    for (const m of (html.matchAll(re))) all.push(parseInt(m[1], 10));
  }
  const filtered = all.filter(n => n > 0 && n <= 1000);
  if (!filtered.length) return null;
  return Math.max(...filtered);
}

function extractRating({ fullText, scriptsJson, fullHTML }) {
  try {
    for (const json of scriptsJson) {
      const obj = JSON.parse(json);
      const arr = Array.isArray(obj) ? obj : [obj];
      for (const it of arr) {
        const v = it?.aggregateRating?.ratingValue;
        if (v != null) {
          const val = parseFloat(String(v).replace(",", "."));
          if (!Number.isNaN(val) && val >= 1 && val <= 5) return val;
        }
      }
    }
  } catch {}
  
  const pool = fullText + "\n" + fullHTML;
  const cands = [
    /([0-9]+[.,][0-9]+)\s+évaluations?/i,
    /★\s*([0-9]+[.,][0-9]+)/i,
    /⭐\s*([0-9]+[.,][0-9]+)/i,
    /Note\s+globale\s*:?\s*([0-9]+[.,][0-9]+)/i,
    /([0-9]+[.,][0-9]+)\s+rating/i,
  ];
  
  for (const re of cands) {
    const m = pool.match(re);
    if (m) {
      const val = parseFloat(m[1].replace(",", "."));
      if (!Number.isNaN(val) && val >= 1 && val <= 5) return val;
    }
  }
  return null;
}

function cleanGeneric(s) {
  if (!s) return null;
  s = s.trim();
  if (/^Airbnb\s*:/.test(s) || /locations de vacances/i.test(s)) return null;
  s = s.replace(/^Quelques informations sur\s+/i, "")
       .replace(/^Profil de\s+/i, "")
       .replace(/^À propos de\s+/i, "")
       .replace(/^About\s+/i, "")
       .replace(/\s*[-–—]\s*Airbnb.*$/i, "");
  s = s.split(/[|•]/)[0].trim();
  if (s.length > 80) s = s.slice(0, 80).trim();
  return s || null;
}

function deepFindName(obj, depth = 0) {
  if (!obj || typeof obj !== "object" || depth > 8) return null;
  const keys = ["fullName","displayName","hostName","publicName","smartName","name","userName","firstName"];
  for (const k of Object.keys(obj)) {
    const v = obj[k];
    if (typeof v === "string" && keys.includes(k)) {
      const c = cleanGeneric(v);
      if (c && !/Airbnb/i.test(c)) return c;
    }
  }
  for (const v of Object.values(obj)) {
    if (v && typeof v === "object") {
      const got = deepFindName(v, depth+1);
      if (got) return got;
    }
  }
  return null;
}

function pickName({ h1Text, fullText, metaTitle, metaDesc, nextData, fullHTML }) {
  try {
    if (nextData) {
      const nd = JSON.parse(nextData);
      const n = deepFindName(nd);
      if (n) return n;
    }
  } catch {}
  
  const candidates = [h1Text, metaTitle, metaDesc].filter(Boolean);

  const fromHtml = (() => {
    const m1 = fullHTML.match(/Quelques informations sur\s*([^<|–—\-]+)/i);
    if (m1) return m1[1];
    const m2 = fullHTML.match(/Profil de\s*([^<|–—\-]+)/i);
    if (m2) return m2[1];
    const m3 = fullHTML.match(/(?:À propos de|About)\s*([^<|–—\-]+)/i);
    if (m3) return m3[1];
    return null;
  })();
  if (fromHtml) candidates.push(fromHtml);

  for (const raw of candidates) {
    const s = String(raw);
    const p = [
      /Quelques informations sur\s+([^|–—\-•\n]+)/i,
      /Profil de\s+([^|–—\-•\n]+)/i,
      /(?:À propos de|About)\s+([^|–—\-•\n]+)/i
    ];
    for (const re of p) {
      const m = s.match(re);
      if (m) {
        const c = cleanGeneric(m[1]);
        if (c) return c;
      }
    }
    const c = cleanGeneric(s);
    if (c) return c;
  }
  
  return null;
}

// ACCEPTER LES COOKIES AIRBNB
async function acceptCookies(page) {
  try {
    // Attendre et cliquer sur le bouton d'acceptation des cookies
    const cookieSelectors = [
      'button:has-text("Tout accepter")',
      'button:has-text("Accepter")',
      'button:has-text("Accept all")',
      'button:has-text("Accept")',
      'button:has-text("OK")',
      'button[data-testid="accept-btn"]',
      '[data-testid="main-cookies-banner-container"] button'
    ];
    
    for (const selector of cookieSelectors) {
      try {
        const button = await page.$(selector);
        if (button) {
          await button.click();
          console.log('  ✓ Cookies acceptés');
          await delay(1000);
          return true;
        }
      } catch {}
    }
  } catch (e) {
    // Pas grave si ça échoue, on continue
  }
  return false;
}

async function scrapeOne(page, url, debugDir, idx, retryCount = 0) {
  const out = { url, name: null, rating: null, joined_year: null, years_active: null, listing_count: null, notes: "" };

  try {
    await page.setUserAgent("Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36");
    await page.setExtraHTTPHeaders({ "Accept-Language": "fr-FR,fr;q=0.9,en;q=0.8" });
    await page.setViewport({ width: 1920, height: 1080 });

    console.log(`  → Navigation vers ${url.substring(0, 60)}...`);
    await page.goto(url, { waitUntil: "domcontentloaded", timeout: 60000 });
    
    // CRITIQUE : Accepter les cookies IMMÉDIATEMENT
    await acceptCookies(page);
    
    // Attendre que body soit chargé
    await page.waitForSelector("body", { timeout: 15000 });
    
    // CRITIQUE : Attendre LONGTEMPS pour la redirection Airbnb
    console.log('  ⏳ Attente de la vraie page (6 secondes)...');
    await delay(6000);
    
    // Vérifier qu'on n'est PAS sur une page d'erreur
    const pageText = await page.evaluate(() => document.body.innerText);
    if (pageText.includes("l'hôte") && pageText.length < 500) {
      throw new Error("Page d'erreur détectée (cookies non acceptés)");
    }
    
    // Scrolls LENTS pour charger tout le contenu
    console.log('  📜 Scroll pour charger le contenu...');
    await page.evaluate(() => window.scrollTo(0, document.body.scrollHeight * 0.3));
    await delay(1500);
    await page.evaluate(() => window.scrollTo(0, document.body.scrollHeight * 0.6));
    await delay(1500);
    await page.evaluate(() => window.scrollTo(0, document.body.scrollHeight));
    await delay(2000);
    await page.evaluate(() => window.scrollTo(0, 0));
    await delay(1500);

    const data = await page.evaluate(() => {
      const q = (sel) => document.querySelector(sel);
      const getAttr = (sel, attr) => q(sel)?.getAttribute(attr) || null;
      const metaTitle = getAttr('meta[property="og:title"]',"content") || getAttr('meta[name="twitter:title"]',"content") || null;
      const metaDesc  = getAttr('meta[name="description"]',"content") || null;

      let h1Text = q("h1")?.innerText || null;
      if (!h1Text) {
        const alt = q('[data-testid*="profile"]');
        h1Text = alt?.textContent || null;
      }

      const fullText = document.body?.innerText || "";
      const fullHTML = document.documentElement?.outerHTML || "";

      const scriptsJson = Array.from(document.querySelectorAll('script[type="application/ld+json"]'))
        .map(s => s.textContent || "");

      let nextData = null;
      try {
        const el = document.querySelector("#__NEXT_DATA__");
        nextData = el ? el.textContent : null;
      } catch {}

      return { metaTitle, metaDesc, h1Text, fullText, fullHTML, scriptsJson, nextData };
    });

    const html = await page.content();
    fs.writeFileSync(path.join(debugDir, `page_${idx+1}.html`), html, "utf8");

    out.name = pickName(data);
    out.rating = extractRating(data);
    out.listing_count = extractListingCount(data.fullText, data.fullHTML);

    // Vérifier si le nom est valide
    if (!out.name || out.name === "l'hôte" || out.name.length < 2) {
      throw new Error("Nom invalide - page non chargée correctement");
    }

    let year = null;
    try {
      if (data.nextData) {
        const nd = JSON.parse(data.nextData);
        const queue = [nd];
        while (queue.length && !year) {
          const cur = queue.shift();
          if (!cur || typeof cur !== "object") continue;
          for (const [k, v] of Object.entries(cur)) {
            if (typeof v === "object" && v) queue.push(v);
            if (year) break;
            const kl = k.toLowerCase();
            if (/(membersince|since|createdat|created_at|joindate|join_date)/.test(kl)) {
              year = yearFromAny(v);
              if (year) break;
            }
          }
        }
      }
    } catch {}
    if (!year) year = extractJoinedYearFromTextOrHtml(data.fullText, data.fullHTML);

    out.joined_year = year || null;
    if (out.joined_year && out.joined_year <= NOW_YEAR) out.years_active = NOW_YEAR - out.joined_year;

    const miss = [];
    for (const k of ["name","rating","joined_year","listing_count"]) if (out[k] == null) miss.push(k);
    if (miss.length) out.notes = `Champs manquants: ${miss.join(", ")}`;

    return out;
  } catch (e) {
    // RETRY automatique (max 2 fois)
    if (retryCount < 2) {
      console.log(`  ⚠️ Erreur: ${e.message} - RETRY ${retryCount + 1}/2`);
      await delay(3000);
      return await scrapeOne(page, url, debugDir, idx, retryCount + 1);
    }
    out.notes = `Erreur après 3 tentatives: ${e?.message || String(e)}`;
    return out;
  }
}

async function main() {
  const urls = readUrls();
  const debugDir = ensureOutDir();
  
  const startTime = Date.now();

  const browser = await puppeteer.launch({
    headless: true,
    args: [
      "--no-sandbox",
      "--disable-setuid-sandbox",
      "--disable-dev-shm-usage",
      "--window-size=1920,1080"
    ]
  });

  const page = await browser.newPage();
  
  // Bloquer images/fonts pour aller plus vite
  await page.setRequestInterception(true);
  page.on("request", req => {
    const t = req.resourceType();
    if (t === "image" || t === "font" || t === "media") req.abort();
    else req.continue();
  });

  const results = [];
  
  for (let i = 0; i < urls.length; i++) {
    console.log(`\n[${i+1}/${urls.length}] Scraping...`);
    const result = await scrapeOne(page, urls[i], debugDir, i);
    results.push(result);
    
    console.log(`  ✓ ${result.name || "?"} | ★${result.rating ?? "?"} | ${result.listing_count ?? "?"} annonces | ${result.joined_year ?? "?"}`);
    if (result.notes) console.log(`  ⚠️ ${result.notes}`);
    
    // Délai entre chaque profil (important pour éviter détection)
    if (i < urls.length - 1) {
      const delayTime = 2000 + Math.random() * 2000; // 2-4 secondes
      console.log(`  ⏳ Attente ${(delayTime/1000).toFixed(1)}s...`);
      await delay(delayTime);
    }
  }

  const csv = Papa.unparse(results, { columns: ["url","name","rating","joined_year","years_active","listing_count","notes"] });
  fs.writeFileSync(OUT_CSV, "\uFEFF" + csv, "utf8");
  
  const duration = ((Date.now() - startTime) / 1000 / 60).toFixed(1);
  const successful = results.filter(r => r.name && r.name !== "l'hôte").length;
  
  console.log(`\n✅ TERMINÉ !`);
  console.log(`📊 ${successful}/${results.length} profils scrapés avec succès`);
  console.log(`⏱️ Durée totale: ${duration} minutes`);
  console.log(`💾 Résultats: ${OUT_CSV}\n`);

  await browser.close();
}

main().catch(err => { console.error(err); process.exit(1); });
