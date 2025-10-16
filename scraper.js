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
  if (!fs.existsSync(INPUT_FILE)) {
    throw new Error(`Fichier urls.txt introuvable: ${INPUT_FILE}`);
  }
  const lines = fs.readFileSync(INPUT_FILE, "utf8")
    .split(/\r?\n/)
    .map(l => l.trim())
    .filter(l => l && !l.startsWith("#"));
  if (lines.length === 0) throw new Error("Aucune URL dans urls.txt");
  return lines;
}

function ensureOutDir() {
  if (!fs.existsSync(OUT_DIR)) fs.mkdirSync(OUT_DIR);
  const debugDir = path.join(OUT_DIR, "debug");
  if (!fs.existsSync(debugDir)) fs.mkdirSync(debugDir);
  return debugDir;
}

function extractJoinedYear(text) {
  const m1 = text.match(/Membre\s+depuis\s+(\d{4})/i);
  if (m1) return parseInt(m1[1], 10);
  const m2 = text.match(/Joined\s+in\s+(\d{4})/i);
  if (m2) return parseInt(m2[1], 10);
  const m3 = text.match(/Member\s+since\s+(\d{4})/i);
  if (m3) return parseInt(m3[1], 10);
  return null;
}

function extractListingCount(text) {
  const patterns = [
    /(\d+)\s+(annonces|hébergements)/i,
    /(\d+)\s+listings/i
  ];
  for (const re of patterns) {
    const m = text.match(re);
    if (m) return parseInt(m[1], 10);
  }
  return null;
}

function extractRating({ fullText, scriptsJson }) {
  try {
    for (const json of scriptsJson) {
      const obj = JSON.parse(json);
      const agg = Array.isArray(obj) ? obj : [obj];
      for (const item of agg) {
        if (item && item.aggregateRating && item.aggregateRating.ratingValue) {
          const val = parseFloat(String(item.aggregateRating.ratingValue).replace(",", "."));
          if (!Number.isNaN(val)) return val;
        }
      }
    }
  } catch {}
  const candidates = [
    /Note\s+globale\s+([0-9]+[.,][0-9]+)/i,
    /([0-9]+[.,][0-9]+)\s*[★\*]/i,
    /([0-9]+[.,][0-9]+)\s*(?:rating|évaluations|reviews)/i,
    /Moyenne\s+de\s+([0-9]+[.,][0-9]+)/i
  ];
  for (const re of candidates) {
    const m = fullText.match(re);
    if (m) {
      const val = parseFloat(m[1].replace(",", "."));
      if (!Number.isNaN(val)) return val;
    }
  }
  return null;
}

function extractName({ metaTitle, h1, metaDesc, fullText }) {
  if (metaTitle) {
    const cleaned = metaTitle
      .replace(/Profil de\s*/i, "")
      .replace(/\s*[-–—]\s*Airbnb.*/i, "")
      .trim();
    if (cleaned && cleaned.length <= 80) return cleaned;
  }
  if (h1) {
    const h = h1.trim();
    if (h && h.length <= 80 && !/Airbnb/i.test(h)) return h;
  }
  if (metaDesc) {
    const m = metaDesc.match(/Profil de\s+([^–—-]+)[–—-]/i);
    if (m) return m[1].trim();
  }
  const m2 = fullText.match(/Profil de\s+([^\n]+)\n/i);
  if (m2) return m2[1].trim();
  return null;
}

async function scrapeOne(page, url, debugDir, idx) {
  const result = {
    url,
    name: null,
    rating: null,
    joined_year: null,
    years_active: null,
    listing_count: null,
    notes: ""
  };

  try {
    const timeoutMs = 90000;

    await page.setUserAgent(
      "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 " +
      "(KHTML, like Gecko) Chrome/119.0.0.0 Safari/537.36"
    );
    await page.setViewport({ width: 1366, height: 900 });

    await page.goto(url, { waitUntil: "networkidle0", timeout: timeoutMs });
    await delay(6000);

    const data = await page.evaluate(() => {
      const getAttr = (sel, attr) => document.querySelector(sel)?.getAttribute(attr) || null;
      const metaTitle = getAttr('meta[property="og:title"]', "content") ||
                        getAttr('meta[name="twitter:title"]', "content") || null;
      const metaDesc  = getAttr('meta[name="description"]', "content") || null;
      const h1 = document.querySelector("h1")?.innerText || null;
      const fullText = document.body?.innerText || "";

      const scriptsJson = Array.from(document.querySelectorAll('script[type="application/ld+json"]'))
        .map(s => s.textContent || "");

      let nextData = null;
      try {
        // @ts-ignore
        nextData = window.__NEXT_DATA__ ? JSON.stringify(window.__NEXT_DATA__) : null;
      } catch {}

      return { metaTitle, metaDesc, h1, fullText, scriptsJson, nextData };
    });

    const html = await page.content();
    const shotPath = path.join(debugDir, `page_${idx + 1}.png`);
    const htmlPath = path.join(debugDir, `page_${idx + 1}.html`);
    fs.writeFileSync(htmlPath, html, "utf8");
    await page.screenshot({ path: shotPath, fullPage: true });

    result.name = extractName(data);
    result.rating = extractRating(data);
    result.joined_year = extractJoinedYear(data.fullText);
    if (result.joined_year && result.joined_year <= NOW_YEAR) {
      result.years_active = NOW_YEAR - result.joined_year;
    }
    result.listing_count = extractListingCount(data.fullText);

    const missing = [];
    for (const key of ["name", "rating", "joined_year", "listing_count"]) {
      if (result[key] == null) missing.push(key);
    }
    if (missing.length) {
      result.notes = `Champs manquants: ${missing.join(", ")}. Voir output/debug/page_${idx + 1}.*`;
    }

    return result;
  } catch (err) {
    result.notes = `Erreur: ${err?.message || String(err)}`;
    return result;
  }
}

async function main() {
  const urls = readUrls();
  const debugDir = ensureOutDir();

  const browser = await puppeteer.launch({
    headless: true,
    args: [
      "--no-sandbox",
      "--disable-setuid-sandbox",
      "--disable-dev-shm-usage",
      "--window-size=1366,900"
    ]
  });

  const page = await browser.newPage();

  const results = [];
  for (let i = 0; i < urls.length; i++) {
    const url = urls[i];
    let res = await scrapeOne(page, url, debugDir, i);

    if (/Champs manquants/.test(res.notes) || /Erreur/.test(res.notes)) {
      await delay(5000);
      res = await scrapeOne(page, url, debugDir, i);
    }
    results.push(res);
    console.log(`[${i + 1}/${urls.length}] ${url} => ${res.name || "?"} | rating ${res.rating ?? "?"} | listings ${res.listing_count ?? "?"}`);
  }

  await browser.close();

  const csv = Papa.unparse(results, {
    columns: ["url", "name", "rating", "joined_year", "years_active", "listing_count", "notes"]
  });
  fs.writeFileSync(OUT_CSV, csv, "utf8");
  console.log(`\nFini. Résultats: ${OUT_CSV}`);
}

main().catch(err => {
  console.error(err);
  process.exit(1);
});
