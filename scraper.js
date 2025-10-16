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
const delay = (ms) => new Promise(res => setTimeout(res, ms));

function readUrls() {
  if (!fs.existsSync(INPUT_FILE)) throw new Error(`Fichier urls.txt introuvable: ${INPUT_FILE}`);
  const lines = fs.readFileSync(INPUT_FILE, "utf8").split(/\r?\n/).map(s=>s.trim()).filter(s=>s && !s.startsWith("#"));
  if (!lines.length) throw new Error("Aucune URL dans urls.txt");
  return Array.from(new Set(lines));
}
function ensureOutDir() {
  if (!fs.existsSync(OUT_DIR)) fs.mkdirSync(OUT_DIR);
  const dbg = path.join(OUT_DIR, "debug");
  if (!fs.existsSync(dbg)) fs.mkdirSync(dbg);
  return dbg;
}

/* ---------------- helpers: parsing ---------------- */
function yearFromAny(val) {
  if (!val) return null;
  // ISO date or number timestamp
  if (typeof val === "number") {
    const d = new Date(val > 1e12 ? val : val * 1000);
    const y = d.getUTCFullYear();
    if (y >= 2007 && y <= NOW_YEAR) return y;
  }
  if (typeof val === "string") {
    // 1) direct 4 digits
    const mYear = val.match(/(?:^|[^0-9])(20\d{2}|19\d{2})(?:[^0-9]|$)/);
    if (mYear) {
      const y = parseInt(mYear[1], 10);
      if (y >= 2007 && y <= NOW_YEAR) return y;
    }
    // 2) ISO date
    const mIso = val.match(/(\d{4})-(\d{2})-(\d{2})/);
    if (mIso) {
      const y = parseInt(mIso[1], 10);
      if (y >= 2007 && y <= NOW_YEAR) return y;
    }
  }
  return null;
}

function extractJoinedYearFromText(text) {
  const pats = [
    /Membre\s+depuis\s+(?:[A-Za-zÀ-ÖØ-öø-ÿ]+\s+)?(\d{4})/i,
    /Depuis\s+(?:[A-Za-zÀ-ÖØ-öø-ÿ]+\s+)?(\d{4})/i,
    /Inscrit[ e]*\s+(?:en|depuis)\s+(?:[A-Za-zÀ-ÖØ-öø-ÿ]+\s+)?(\d{4})/i,
    /Joined\s+in\s+(?:[A-Za-z]+\s+)?(\d{4})/i,
    /Member\s+since\s+(?:[A-Za-z]+\s+)?(\d{4})/i,
    /On\s+Airbnb\s+since\s+(?:[A-Za-z]+\s+)?(\d{4})/i
  ];
  for (const re of pats) {
    const m = text.match(re);
    if (m) {
      const y = parseInt(m[1], 10);
      if (y >= 2007 && y <= NOW_YEAR) return y;
    }
  }
  // fallback: nearest year around words
  const around = text.match(/(?:membre|since|joined|inscrit)[^0-9]{0,20}(20\d{2}|19\d{2})/i);
  if (around) {
    const y = parseInt(around[1], 10);
    if (y >= 2007 && y <= NOW_YEAR) return y;
  }
  return null;
}

function extractListingCount(text) {
  const pats = [
    /(\d{1,5})\s+(annonces|hébergements)/i,
    /(\d{1,5})\s+listings?/i
  ];
  for (const re of pats) {
    const m = text.match(re);
    if (m) return parseInt(m[1], 10);
  }
  return null;
}

function extractRating({ fullText, scriptsJson }) {
  try {
    for (const json of scriptsJson) {
      const obj = JSON.parse(json);
      const arr = Array.isArray(obj) ? obj : [obj];
      for (const it of arr) {
        const v = it?.aggregateRating?.ratingValue;
        if (v != null) {
          const val = parseFloat(String(v).replace(",", "."));
          if (!Number.isNaN(val)) return val;
        }
      }
    }
  } catch {}
  const cands = [
    /Note\s+globale\s+([0-9]+[.,][0-9]+)/i,
    /Moyenne\s+de\s+([0-9]+[.,][0-9]+)/i,
    /([0-9]+[.,][0-9]+)\s*(?:évaluations|reviews|rating)/i,
    /([0-9]+[.,][0-9]+)\s*[★\*]/i
  ];
  for (const re of cands) {
    const m = fullText.match(re);
    if (m) {
      const val = parseFloat(m[1].replace(",", "."));
      if (!Number.isNaN(val)) return val;
    }
  }
  return null;
}

function cleanGeneric(s) {
  if (!s) return null;
  s = s.trim();
  if (/^Airbnb\s*:/.test(s) || /locations de vacances/i.test(s)) return null; // ignorer titres génériques
  s = s.replace(/^Quelques informations sur\s+/i, "");
  s = s.replace(/^Profil de\s+/i, "");
  s = s.replace(/^À propos de\s+/i, "");
  s = s.replace(/^About\s+/i, "");
  s = s.replace(/\s*[-–—]\s*Airbnb.*$/i, "");
  s = s.split("|")[0].trim();
  if (s.length > 80) s = s.slice(0, 80).trim();
  return s || null;
}
function deepFindName(obj, depth = 0) {
  if (!obj || typeof obj !== "object" || depth > 6) return null;
  const keys = ["fullName","displayName","hostName","publicName","name","userName","firstName"];
  for (const k of Object.keys(obj)) {
    const v = obj[k];
    if (typeof v === "string" && keys.includes(k)) {
      const c = cleanGeneric(v);
      if (c && !/Airbnb/i.test(c)) return c;
    }
  }
  for (const k of Object.keys(obj)) {
    const v = obj[k];
    if (v && typeof v === "object") {
      const got = deepFindName(v, depth+1);
      if (got) return got;
    }
  }
  return null;
}
function pickName({ h1Text, fullText, metaTitle, metaDesc, nextData }) {
  try {
    if (nextData) {
      const nd = JSON.parse(nextData);
      const n = deepFindName(nd);
      if (n) return n;
    }
  } catch {}
  if (h1Text) {
    const n = cleanGeneric(h1Text) || (h1Text.match(/Quelques informations sur\s+(.+)/i)?.[1]);
    if (n) {
      const c = cleanGeneric(n);
      if (c) return c;
    }
  }
  const t = fullText.match(/(?:Quelques informations sur|Profil de)\s+([^\n\|]+)(?:\s*\||\n|$)/i);
  if (t) {
    const c = cleanGeneric(t[1]);
    if (c) return c;
  }
  if (metaDesc) {
    const m = metaDesc.match(/Profil de\s+([^–—\-\|]+)[–—\-\|]/i);
    if (m) {
      const c = cleanGeneric(m[1]);
      if (c) return c;
    }
  }
  if (metaTitle) {
    const c = cleanGeneric(metaTitle);
    if (c) return c;
  }
  return null;
}

/* ---------------- core: navigation + scrape ---------------- */
async function gotoRobust(page, url) {
  const timeoutMs = 120000;
  try {
    await page.goto(url, { waitUntil: "networkidle0", timeout: timeoutMs });
    return;
  } catch (_) {
    await page.goto(url, { waitUntil: "domcontentloaded", timeout: timeoutMs });
  }
}

async function scrapeOne(page, url, debugDir, idx) {
  const out = { url, name: null, rating: null, joined_year: null, years_active: null, listing_count: null, notes: "" };

  try {
    await page.setUserAgent("Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/119.0.0.0 Safari/537.36");
    await page.setExtraHTTPHeaders({ "Accept-Language": "fr-FR,fr;q=0.9,en;q=0.8" });
    await page.setViewport({ width: 1366, height: 900 });

    // Bloquer les ressources lourdes
    await page.setRequestInterception(true);
    page.on("request", req => {
      const type = req.resourceType();
      if (type === "image" || type === "media" || type === "font" || type === "stylesheet") req.abort();
      else req.continue();
    });

    await gotoRobust(page, url);
    await page.waitForSelector("body", { timeout: 15000 });

    // Scroll pour déclencher lazy
    await page.evaluate(() => { window.scrollTo(0, document.body.scrollHeight * 0.5); });
    await delay(800);
    await page.evaluate(() => { window.scrollTo(0, document.body.scrollHeight); });
    await delay(1200);
    await page.evaluate(() => { window.scrollTo(0, 0); });
    await delay(500);

    const data = await page.evaluate(() => {
      const q = (sel) => document.querySelector(sel);
      const getAttr = (sel, attr) => q(sel)?.getAttribute(attr) || null;
      const metaTitle = getAttr('meta[property="og:title"]',"content") || getAttr('meta[name="twitter:title"]',"content") || null;
      const metaDesc  = getAttr('meta[name="description"]',"content") || null;
      let h1Text = q("h1")?.innerText || null;
      if (!h1Text) {
        const alt = q('[data-testid="user-profile__heading"], [data-testid="user-profile-heading"]');
        h1Text = alt?.textContent || null;
      }
      const fullText = document.body?.innerText || "";
      const scriptsJson = Array.from(document.querySelectorAll('script[type="application/ld+json"]')).map(s => s.textContent || "");
      let nextData = null;
      try { /* Next.js payload */ // @ts-ignore
        nextData = window.__NEXT_DATA__ ? JSON.stringify(window.__NEXT_DATA__) : null;
      } catch {}
      return { metaTitle, metaDesc, h1Text, fullText, scriptsJson, nextData };
    });

    // Debug
    const html = await page.content();
    fs.writeFileSync(path.join(debugDir, `page_${idx+1}.html`), html, "utf8");
    await page.screenshot({ path: path.join(debugDir, `page_${idx+1}.png`), fullPage: true });

    // Name
    out.name = pickName(data);

    // Rating
    out.rating = extractRating(data);

    // Listings
    out.listing_count = extractListingCount(data.fullText);

    // Joined year from NEXT then text
    let joined = null;
    try {
      if (data.nextData) {
        const nd = JSON.parse(data.nextData);
        // Chercher clés fréquentes
        const stack = [nd];
        let foundRaw = null;
        const keys = ["memberSince","since","createdAt","created_at","joinDate","join_date"];
        while (stack.length && !foundRaw) {
          const cur = stack.pop();
          if (cur && typeof cur === "object") {
            for (const k of Object.keys(cur)) {
              const v = cur[k];
              if (keys.includes(k)) { foundRaw = v; break; }
              if (v && typeof v === "object") stack.push(v);
            }
          }
        }
        joined = yearFromAny(foundRaw);
      }
    } catch {}
    if (!joined) joined = extractJoinedYearFromText(data.fullText);

    out.joined_year = joined;
    if (out.joined_year && out.joined_year <= NOW_YEAR) out.years_active = NOW_YEAR - out.joined_year;

    const miss = [];
    for (const k of ["name","rating","joined_year","listing_count"]) if (out[k] == null) miss.push(k);
    if (miss.length) out.notes = `Champs manquants: ${miss.join(", ")}. Voir output/debug/page_${idx+1}.*`;
    return out;
  } catch (e) {
    out.notes = `Erreur: ${e?.message || String(e)}`;
    return out;
  } finally {
    // retirer l'interception pour la prochaine itération proprement
    try { page.removeAllListeners("request"); await page.setRequestInterception(false); } catch {}
  }
}

async function main() {
  const urls = readUrls();
  const debugDir = ensureOutDir();

  const browser = await puppeteer.launch({
    headless: true,
    args: ["--no-sandbox","--disable-setuid-sandbox","--disable-dev-shm-usage","--window-size=1366,900"]
  });
  const page = await browser.newPage();

  const results = [];
  for (let i = 0; i < urls.length; i++) {
    let r = await scrapeOne(page, urls[i], debugDir, i);
    if (/Erreur: Navigation timeout/i.test(r.notes)) {
      await delay(1500);
      r = await scrapeOne(page, urls[i], debugDir, i); // retry complet
    }
    results.push(r);
    console.log(`[${i+1}/${urls.length}] ${urls[i]} => ${r.name || "?"} | rating ${r.rating ?? "?"} | listings ${r.listing_count ?? "?"} | joined ${r.joined_year ?? "?"} | years ${r.years_active ?? "?"}`);
    await delay(700);
  }

  const csv = Papa.unparse(results, {
    columns: ["url","name","rating","joined_year","years_active","listing_count","notes"]
  });
  fs.writeFileSync(OUT_CSV, "\uFEFF" + csv, "utf8"); // BOM pour Excel
  console.log(`\nFini. Résultats: ${OUT_CSV}`);

  await browser.close();
}

main().catch(err => { console.error(err); process.exit(1); });
