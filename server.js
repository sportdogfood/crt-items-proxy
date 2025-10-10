// server.js — header and helpers (FULL FILE)

// Core imports
const express = require("express");
const fetch = require("node-fetch");
const cors = require("cors");
const morgan = require("morgan");
const path = require("path");
const fs = require("fs");
require("dotenv").config();

// App
const app = express();

// --- Config via env ---
const UPSTREAM_BASE = process.env.UPSTREAM_BASE; // e.g. https://raw.githubusercontent.com/sportdogfood/clear-round-datasets/main
if (!UPSTREAM_BASE) throw new Error("Missing UPSTREAM_BASE");

// Allowed top-level dirs for /items/*
const ALLOW_DIRS = new Set(
  (process.env.ALLOW_DIRS || "items,items/runners,items/tasks,items/brand,items/venues,items/events,items/schema,items/gold,items/policy,docs,assets,images")
    .split(",")
    .map(s => s.trim())
    .filter(Boolean)
);

// Domain allowlists
const ALLOW_ORIGINS = new Set(
  (process.env.ALLOW_ORIGINS || "https://items.clearroundtravel.com,https://blog.clearroundtravel.com")
    .split(",")
    .map(s => s.trim())
    .filter(Boolean)
);

const ALLOW_UPSTREAM_HOSTS = new Set(
  (process.env.ALLOW_UPSTREAM_HOSTS || "raw.githubusercontent.com")
    .split(",")
    .map(s => s.trim().toLowerCase())
    .filter(Boolean)
);

const CACHE_TTL    = parseInt(process.env.CACHE_TTL || "300", 10);
const STASH_PREFIX = process.env.STASH_PREFIX || "items/stash";
const GITHUB_REPO   = process.env.GITHUB_REPO;      // e.g. sportdogfood/clear-round-datasets
const GITHUB_BRANCH = process.env.GITHUB_BRANCH || "main";
const GITHUB_TOKEN  = process.env.GITHUB_TOKEN || "";

// --- Tiny in-memory cache ---
const memoryCache = new Map(); // key => { body, etag, lastModified, fetchedAt }

// --- helpers ---
const ALLOWED_FILE_EXT_RE = /\.(json|txt|md|html|js|csv|ndjson)$/i;

function isAllowedPath(rel) {
  if (!rel) return false;
  if (rel.includes("..")) return false;
  if (!/^[a-z0-9][a-z0-9\-_.\/]*\.[a-z0-9]+$/i.test(rel)) return false;
  if (!ALLOWED_FILE_EXT_RE.test(rel)) return false;
  const top = rel.split("/")[0];
  return ALLOW_DIRS.has(top);
}

function upstreamUrl(rel) {
  return `${UPSTREAM_BASE}/${rel}`
    .replace(/(?<!:)\/{2,}/g, "/")
    .replace("https:/", "https://");
}

function contentTypeFor(rel) {
  const ext = rel.toLowerCase().slice(rel.lastIndexOf("."));
  switch (ext) {
    case ".json":   return "application/json; charset=utf-8";
    case ".txt":    return "text/plain; charset=utf-8";
    case ".md":     return "text/markdown; charset=utf-8";
    case ".html":   return "text/html; charset=utf-8";
    case ".js":     return "application/javascript; charset=utf-8";
    case ".csv":    return "text/csv; charset=utf-8";
    case ".ndjson": return "application/x-ndjson; charset=utf-8";
    default:        return "application/octet-stream";
  }
}

// Enforce upstream host allowlist
function assertAllowedUpstream(urlStr) {
  let host;
  try { host = new URL(urlStr).host.toLowerCase(); }
  catch { const e = new Error("Invalid upstream URL"); e.code = 400; throw e; }
  if (!ALLOW_UPSTREAM_HOSTS.has(host)) {
    const e = new Error(`Upstream host not allowed: ${host}`);
    e.code = 400;
    throw e;
  }
}

// Minimal fetch with timeout + single retry
async function fetchWithRetry(url, options = {}, { attempts = 2, timeoutMs = 5000 } = {}) {
  let lastErr;
  for (let i = 0; i < attempts; i++) {
    const controller = new AbortController();
    const timer = setTimeout(() => controller.abort(), timeoutMs);
    try {
      const resp = await fetch(url, { ...options, signal: controller.signal });
      clearTimeout(timer);
      if (resp.status >= 500 && resp.status <= 599 && i + 1 < attempts) {
        await new Promise(r => setTimeout(r, 400 * (i + 1)));
        continue;
      }
      return resp;
    } catch (e) {
      clearTimeout(timer);
      lastErr = e;
      if (i + 1 < attempts) {
        await new Promise(r => setTimeout(r, 400 * (i + 1)));
        continue;
      }
      throw lastErr;
    }
  }
  throw lastErr;
}

// Safe path segments for /stash
function safeSeg(s, allowDots = false) {
  const re = allowDots ? /^[a-z0-9._-]+$/i : /^[a-z0-9_-]+$/i;
  return typeof s === "string" && re.test(s) ? s : null;
}

function joinStashPath(folder, base, name) {
  const f = safeSeg(folder) || "misc";
  const b = safeSeg(base) || new Date().toISOString().replace(/[:.]/g, "").slice(0, 15);
  const n = safeSeg(name, /*allowDots*/ true);
  if (!n) return null;
  return `${STASH_PREFIX}/${f}/${b}/${n}`.replace(/^items\//, "items/");
}

async function commitText(rel, bodyText, message) {
  if (!GITHUB_REPO || !GITHUB_TOKEN) {
    const e = new Error("Missing GITHUB_REPO or GITHUB_TOKEN");
    e.code = 500;
    throw e;
  }
  if (!isAllowedPath(rel)) {
    const e = new Error("Path not allowed");
    e.code = 400;
    throw e;
  }

  const repoPath = `items/${String(rel).replace(/^items\//, "")}`;
  const api = `https://api.github.com/repos/${GITHUB_REPO}/contents/${repoPath}`;
  const headers = {
    Authorization: `Bearer ${GITHUB_TOKEN}`,
    Accept: "application/vnd.github+json",
    "User-Agent": "crt-items-proxy"
  };

  // existing SHA
  let sha;
  const head = await fetchWithRetry(`${api}?ref=${encodeURIComponent(GITHUB_BRANCH)}`, { headers }, { attempts: 2, timeoutMs: 5000 });
  if (head.ok) {
    const meta = await head.json();
    sha = meta.sha;
  }

  const contentStr = String(bodyText).replace(/\r\n?/g, "\n");
  const body = {
    message: message || `stash: ${repoPath}`,
    content: Buffer.from(contentStr, "utf8").toString("base64"),
    branch: GITHUB_BRANCH
  };
  if (sha) body.sha = sha;

  const put = await fetchWithRetry(api, { method: "PUT", headers, body: JSON.stringify(body) }, { attempts: 2, timeoutMs: 7000 });
  const result = await put.json();
  if (!put.ok) {
    const e = new Error(result.message || "GitHub put failed");
    e.code = put.status;
    e.details = result;
    throw e;
  }

  memoryCache.delete(rel);
  return { path: repoPath, commit: result.commit && { sha: result.commit.sha, url: result.commit.html_url } };
}

// Manifest helpers
const RAW_RE = /^https:\/\/raw\.githubusercontent\.com\/([^/]+)\/([^/]+)\/([^/]+)\/(.+)$/;
const m = RAW_RE.exec(UPSTREAM_BASE);
let GH_OWNER, GH_REPO, GH_BRANCH, GH_BASEPATH;

console.log(`[startup] UPSTREAM_BASE='${UPSTREAM_BASE}'`);
console.log(`[startup] ALLOW_DIRS=${Array.from(ALLOW_DIRS).join(",")}`);
console.log(`[startup] ALLOW_ORIGINS=${Array.from(ALLOW_ORIGINS).join(",")}`);
console.log(`[startup] ALLOW_UPSTREAM_HOSTS=${Array.from(ALLOW_UPSTREAM_HOSTS).join(",")}`);

if (m) {
  [, GH_OWNER, GH_REPO, GH_BRANCH, GH_BASEPATH] = m;
  console.log(`[manifest] enabled for ${GH_OWNER}/${GH_REPO}@${GH_BRANCH} base='${GH_BASEPATH}'`);
} else {
  console.warn("[manifest] disabled: UPSTREAM_BASE is not raw.githubusercontent.com");
}

async function listDir(dir) {
  if (!GH_OWNER) throw new Error("Manifest unavailable (UPSTREAM_BASE not raw.github...)");
  const key = `__index__/${dir}`;
  const now = Date.now();
  const cached = memoryCache.get(key);
  const TTL_MS = 24 * 60 * 60 * 1000;
  if (cached && now - cached.fetchedAt < TTL_MS) return cached.body;

  const api = `https://api.github.com/repos/${GH_OWNER}/${GH_REPO}/contents/${GH_BASEPATH}/${dir}?ref=${encodeURIComponent(GH_BRANCH)}`;
  const headers = { Accept: "application/vnd.github+json", "User-Agent": "crt-items-proxy" };
  if (GITHUB_TOKEN) headers.Authorization = `Bearer ${GITHUB_TOKEN}`;

  const r = await fetchWithRetry(api, { headers }, { attempts: 2, timeoutMs: 5000 });
  if (!r.ok) throw new Error(`GitHub list error ${r.status}: ${await r.text()}`);
  const json = await r.json();

  const files = Array.isArray(json)
    ? json.filter(x => x.type === "file" && /\.json$/i.test(x.name)).map(x => x.name.replace(/\.json$/i, "")).sort()
    : [];

  const body = { dir, count: files.length, files };
  memoryCache.set(key, { body, fetchedAt: now });
  return body;
}

// ------------------------------
// Build from trigger (ONE change)
// ------------------------------
async function buildFilesFromTrigger(trigger) {
  // trigger: { add_post, canonical_base, images_link?, allow_urls?[] }
  if (!trigger || !trigger.add_post) {
    const e = new Error("trigger.add_post required"); e.code = 400; throw e;
  }
  const addUrl = String(trigger.add_post).trim();
  if (!/^https:\/\/raw\.githubusercontent\.com\//i.test(addUrl)) {
    const e = new Error("add_post must be raw.githubusercontent.com"); e.code = 400; throw e;
  }

  // Fetch the post JSON (verbatim body for write-through)
  const postResp = await fetchWithRetry(addUrl, {}, { attempts: 2, timeoutMs: 6000 });
  if (!postResp.ok) { const t = await postResp.text(); const e = new Error(`add_post fetch ${postResp.status}: ${t}`); e.code = 400; throw e; }
  const postText = await postResp.text();
  let postJson;
  try { postJson = JSON.parse(postText); } catch (e) {
    const err = new Error(`add_post invalid JSON: ${e.message}`); err.code = 400; throw err;
  }

  // Parse venue/year/slug from filename + parent folder
  // Expect: .../docs/blogs/{venue}-blogs-(YYYY)/{slug}/{slug}.json
  const mm = addUrl.match(/\/docs\/blogs\/([^/]+)-blogs-(\d{4})\/([^/]+)\/\3\.json$/i);
  if (!mm) { const e = new Error("add_post path does not match expected blog structure"); e.code = 400; throw e; }
  const venue = mm[1], year = mm[2], slug = mm[3];

  // Derive canonical + paths
  const canonicalBase = String(trigger.canonical_base || "https://blog.clearroundtravel.com").replace(/\/+$/,"");
  const postPath = `docs/blogs/${venue}-blogs-${year}/${slug}/`;
  const jsonRel  = `${postPath}${slug}.json`;
  const indexRel = `${postPath}index.html`;
  const manifestRel = `docs/blogs/manifest.json`;
  const rssRel = `docs/blogs/rss.xml`;
  const sitemapRel = `docs/sitemap.xml`;

  // Read existing manifest
  let manifest = [];
  try {
    const manUrl = `${UPSTREAM_BASE.replace(/\/+$/,"")}/${manifestRel}`;
    const manResp = await fetchWithRetry(manUrl, {}, { attempts: 2, timeoutMs: 5000 });
    if (manResp.ok) manifest = await manResp.json();
  } catch {}
  if (!Array.isArray(manifest)) manifest = [];

  // Compute minimal fields
  const title =
    postJson?.seo?.section_title ||
    postJson?.seo?.open_graph_title ||
    postJson?.seo?.search_title ||
    slug;

  // prefer filename date in slug: {venue}-blog-YYYY-MM-DD
  const dateMatch = slug.match(/-(\d{4}-\d{2}-\d{2})$/);
  const date = dateMatch ? dateMatch[1] : (postJson?.meta?.timestamp_iso || "").slice(0,10);
  const month_num = date ? date.slice(5,7) : "";
  const month_name = month_num ? ["","January","February","March","April","May","June","July","August","September","October","November","December"][parseInt(month_num,10)] : "";
  const season = (() => {
    const m = parseInt(month_num||"0",10);
    if (m===12||m===1||m===2) return "winter";
    if (m>=3 && m<=5) return "spring";
    if (m>=6 && m<=8) return "summer";
    if (m>=9 && m<=11) return "fall";
    return "";
  })();

  // Optional images_link for all slots (hero/stay/dine/essentials/locale/card)
  let imgs = {};
  if (trigger.images_link) {
    try {
      const imgResp = await fetchWithRetry(trigger.images_link, {}, { attempts: 2, timeoutMs: 5000 });
      if (imgResp.ok) imgs = await imgResp.json();
    } catch {}
  }
  const flagOff = (v) => String(v||"0") === "1";
  const heroSrc = flagOff(imgs.hero_image_hidden) ? "" : (imgs.hero_image_link || "");
  const staySrc = flagOff(imgs.stay_image_hidden) ? "" : (imgs.stay_image_link || "");
  const dineSrc = flagOff(imgs.dine_image_hidden) ? "" : (imgs.dine_image_link || "");
  const essSrc  = flagOff(imgs.essentials_image_hidden) ? "" : (imgs.essentials_image_link || "");
  const locSrc  = flagOff(imgs.locale_image_hidden) ? "" : (imgs.locale_image_link || "");
  const cardSrc = flagOff(imgs.card_image_hidden) ? "" : (imgs.card_image_link || "");
  const imgAlt  = String(imgs.image_alt_text || "placeholder");

  // Upsert manifest entry
  const pathPublic = `/blogs/${venue}-blogs-${year}/${slug}/`;
  const jsonPublic = `/${jsonRel.replace(/^docs\//,"")}`;
  const entry = {
    slug, title, date,
    year, month_num, month_name, season,
    path: pathPublic,
    json: jsonPublic,
    image: cardSrc || "/docs/assets/images/card-placeholder.jpg",
    venue
  };
  const idx = manifest.findIndex(x => x.slug === slug);
  if (idx >= 0) manifest[idx] = { ...manifest[idx], ...entry };
  else manifest.push(entry);

  // ---------- RENDER: Single post HTML (full content; replaces placeholder) ----------
  const esc = (s="") => String(s)
    .replace(/&/g,"&amp;").replace(/</g,"&lt;").replace(/>/g,"&gt;")
    .replace(/"/g,"&quot;").replace(/'/g,"&#39;");

  const linkify = (s="") =>
    String(s).replace(/\[([^\]]+)\]\((https?:\/\/[^)]+)\)/g,
      (_m, label, url) => `<a href="${esc(url)}" target="_blank" rel="noopener">${esc(label)}</a>`);

  const listLinks = (arr=[]) => arr.map(it => {
    const name = esc(it.name || "");
    const alt  = it.alt ? ` — ${esc(it.alt)}` : "";
    const href = it.link && /^https?:\/\//i.test(it.link) ? esc(it.link) : "#";
    return `<li><a href="${href}" target="_blank" rel="noopener">${name}</a>${alt}</li>`;
  }).join("");

  const styles = `
    <style>
      :root{--bg:#0f1214;--panel:#161a1d;--text:#eaeef2;--muted:#a9b3be;--brand:#7fb3ff;--ring:rgba(255,255,255,.07)}
      *{box-sizing:border-box}
      body{margin:0;background:var(--bg);color:var(--text);font-family:system-ui,-apple-system,"Segoe UI",Roboto,Ubuntu,"Helvetica Neue",Arial}
      main{max-width:1100px;margin:0 auto;padding:28px 20px 80px}
      header.h1{display:flex;flex-direction:column;gap:10px;margin:16px 0 18px}
      h1{font-size:28px;line-height:1.15;margin:0}
      p.lede{color:var(--muted);margin:0}
      .hero{border-radius:20px;overflow:hidden;background:#0a0d0f;border:1px solid var(--ring);margin:8px 0 22px}
      .hero img{display:block;width:100%;height:360px;object-fit:cover}
      section.block{background:var(--panel);border:1px solid var(--ring);border-radius:18px;margin:14px 0;padding:18px}
      section.block h2{font-size:18px;margin:0 0 8px}
      section.block p{margin:0 0 10px;color:var(--text)}
      ul.links{margin:8px 0 0 18px;padding:0}
      ul.links li{margin:6px 0}
      .thumb{width:100%;height:180px;object-fit:cover;border-radius:12px;border:1px solid var(--ring);margin:8px 0}
      a{color:var(--brand);text-decoration:none}
      nav a{color:var(--brand)}
      nav.breadcrumbs{max-width:1100px;margin:0 auto;padding:8px 20px;color:var(--muted);font-size:14px}
      footer{color:var(--muted);font-size:12px;margin-top:26px}
      @media (max-width:720px){ .hero img{height:220px} }
    </style>
  `;

  const seoDesc =
    postJson?.seo?.meta_description ||
    postJson?.seo?.open_graph_description ||
    postJson?.seo?.search_description || "";

  const postHtml = `<!doctype html><html><head><meta charset="utf-8">
    <title>${esc(title)}</title>
    <meta name="viewport" content="width=device-width,initial-scale=1">
    <link rel="canonical" href="${esc(canonicalBase + pathPublic)}">
    ${seoDesc ? `<meta name="description" content="${esc(seoDesc)}">` : ""}
    ${styles}
  </head><body>
    <nav class="breadcrumbs"><a href="/blogs/">Blogs</a> / <a href="/blogs/${esc(year)}/">${esc(year)}</a></nav>
    <main>
      <header class="h1">
        <h1>${esc(title)}</h1>
        ${date ? `<p class="lede">${esc(date)} • ${esc(venue.toUpperCase())}</p>` : ""}
      </header>

      ${heroSrc ? `<div class="hero"><img src="${esc(heroSrc)}" alt="${esc(imgAlt)}"></div>` : ""}

      ${postJson?.hello?.intro ? `<section class="block"><h2>Overview</h2><p>${linkify(postJson.hello.intro)}</p>${postJson.hello.transition?`<p>${linkify(postJson.hello.transition)}</p>`:""}</section>`:""}

      ${postJson?.stay?.title || postJson?.stay?.paragraph ? `
      <section class="block">
        <h2>${esc(postJson.stay.title || "Stay")}</h2>
        ${staySrc?`<img class="thumb" src="${esc(staySrc)}" alt="${esc(imgAlt)}">`:""}
        ${postJson.stay.paragraph?`<p>${linkify(postJson.stay.paragraph)}</p>`:""}
        ${Array.isArray(postJson.stay_items)&&postJson.stay_items.length?`<ul class="links">${listLinks(postJson.stay_items)}</ul>`:""}
        ${postJson.stay.spectator_tip?`<p><em>${esc(postJson.stay.spectator_tip)}</em></p>`:""}
      </section>` : ""}

      ${postJson?.dine?.title || postJson?.dine?.paragraph ? `
      <section class="block">
        <h2>${esc(postJson.dine.title || "Dine")}</h2>
        ${dineSrc?`<img class="thumb" src="${esc(dineSrc)}" alt="${esc(imgAlt)}">`:""}
        ${postJson.dine.paragraph?`<p>${linkify(postJson.dine.paragraph)}</p>`:""}
        ${Array.isArray(postJson.dine_items)&&postJson.dine_items.length?`<ul class="links">${listLinks(postJson.dine_items)}</ul>`:""}
        ${postJson.dine.spectator_tip?`<p><em>${esc(postJson.dine.spectator_tip)}</em></p>`:""}
      </section>` : ""}

      ${postJson?.locale?.title || postJson?.locale?.paragraph ? `
      <section class="block">
        <h2>${esc(postJson.locale.title || "Locale")}</h2>
        ${locSrc?`<img class="thumb" src="${esc(locSrc)}" alt="${esc(imgAlt)}">`:""}
        ${postJson.locale.paragraph?`<p>${linkify(postJson.locale.paragraph)}</p>`:""}
        ${postJson.locale.spectator_tip?`<p><em>${esc(postJson.locale.spectator_tip)}</em></p>`:""}
      </section>` : ""}

      ${postJson?.essentials?.title || postJson?.essentials?.paragraph ? `
      <section class="block">
        <h2>${esc(postJson.essentials.title || "Essentials")}</h2>
        ${essSrc?`<img class="thumb" src="${esc(essSrc)}" alt="${esc(imgAlt)}">`:""}
        ${postJson.essentials.paragraph?`<p>${linkify(postJson.essentials.paragraph)}</p>`:""}
        ${Array.isArray(postJson.essentials_items)&&postJson.essentials_items.length?`<ul class="links">${listLinks(postJson.essentials_items)}</ul>`:""}
        ${postJson.essentials.spectator_tip?`<p><em>${esc(postJson.essentials.spectator_tip)}</em></p>`:""}
      </section>` : ""}

      ${postJson?.outro?.outro_pivot || postJson?.outro?.outro_main ? `
      <section class="block">
        <h2>Wrap-Up</h2>
        ${postJson.outro.outro_pivot?`<p>${linkify(postJson.outro.outro_pivot)}</p>`:""}
        ${postJson.outro.outro_main?`<p>${linkify(postJson.outro.outro_main)}</p>`:""}
      </section>` : ""}

      <footer>Generated by Structure Runner • <a href="${esc(jsonPublic)}" target="_blank" rel="noopener">${esc(slug)}.json</a></footer>
    </main>
  </body></html>\n`;

  // ---------- INDEXES / RSS / SITEMAP ----------
  const bySeason = (list) => ({
    winter: list.filter(x => x.season === "winter"),
    spring: list.filter(x => x.season === "spring"),
    summer: list.filter(x => x.season === "summer"),
    fall:   list.filter(x => x.season === "fall"),
  });

  const renderCards = (items) => items.map(it => `
    <article class="card">
      <a href="${it.path}">
        <img loading="lazy" src="${it.image || "/docs/assets/images/card-placeholder.jpg"}" alt="${(it.title||"").replace(/"/g,"&quot;")}">
        <h3>${esc(it.title || it.slug)}</h3>
        <p>${esc(it.date || "")}</p>
      </a>
    </article>`).join("");

  const renderSection = (label, items) => items.length ? `
    <section class="section">
      <header><h2>${label}</h2><a class="see-all" href="/blogs/?season=${label.toLowerCase()}">See all &rsaquo;</a></header>
      <div class="carousel">${renderCards(items)}</div>
    </section>` : "";

  const listStyles = `
    <style>
      body{margin:0;background:#111315;color:#f1f1f1;font-family:system-ui,"Segoe UI",sans-serif}
      main{max-width:1100px;margin:0 auto;padding:40px 20px}
      .section{margin:28px 0}
      .section header{display:flex;justify-content:space-between;align-items:center;margin-bottom:12px}
      .carousel{display:grid;grid-auto-flow:column;grid-auto-columns:minmax(230px,1fr);gap:16px;overflow-x:auto;padding-bottom:6px}
      .card{background:#1a1d1f;border-radius:16px;box-shadow:0 0 0 1px rgba(255,255,255,.06) inset}
      .card img{width:100%;height:148px;object-fit:cover;border-top-left-radius:16px;border-top-right-radius:16px;display:block}
      .card h3{font-size:16px;margin:10px 12px 6px}
      .card p{font-size:13px;color:#a0a0a0;margin:0 12px 12px}
      a{color:#8bb7ff;text-decoration:none}
      .see-all{font-size:14px;color:#a0c4ff}
      nav.breadcrumbs{max-width:1100px;margin:0 auto;padding:12px 20px;color:#a0a0a0;font-size:14px}
    </style>
  `;

  const allSorted = manifest.slice().sort((a,b)=>String(b.date).localeCompare(a.date));
  const seasonsAll = bySeason(allSorted);
  const blogsIndexHtml = `<!doctype html><html><head><meta charset="utf-8">
    <title>Blogs</title><meta name="viewport" content="width=device-width,initial-scale=1">${listStyles}</head>
    <body><nav class="breadcrumbs">Home / Blogs</nav><main>
      ${renderSection("Fall",   seasonsAll.fall)}
      ${renderSection("Summer", seasonsAll.summer)}
      ${renderSection("Spring", seasonsAll.spring)}
      ${renderSection("Winter", seasonsAll.winter)}
    </main></body></html>\n`;

  const yearList = allSorted.filter(x => x.year === year);
  const seasonsYear = bySeason(yearList);
  const yearIndexHtml = `<!doctype html><html><head><meta charset="utf-8">
    <title>Blogs ${year}</title><meta name="viewport" content="width=device-width,initial-scale=1">${listStyles}</head>
    <body><nav class="breadcrumbs">Home / Blogs / ${esc(year)}</nav><main>
      ${renderSection("Fall",   seasonsYear.fall)}
      ${renderSection("Summer", seasonsYear.summer)}
      ${renderSection("Spring", seasonsYear.spring)}
      ${renderSection("Winter", seasonsYear.winter)}
    </main></body></html>\n`;

  const rss = `<?xml version="1.0" encoding="UTF-8"?><rss version="2.0"><channel><title>Blog RSS</title>${
    allSorted.slice(0,20).map(it=>`<item><title>${esc(it.title||it.slug)}</title><link>${esc(canonicalBase+it.path)}</link><pubDate>${esc(it.date||"")}</pubDate></item>`).join("")
  }</channel></rss>\n`;

  const smUrls = [
    `${canonicalBase}/blogs/`,
    ...manifest.map(it => `${canonicalBase}${it.path}`),
    ...Array.from(new Set(manifest.map(it => it.year))).map(y => `${canonicalBase}/blogs/${y}/`)
  ];
  const sitemap = `<?xml version="1.0" encoding="UTF-8"?><urlset xmlns="http://www.sitemaps.org/schemas/sitemap/0.9">${
    smUrls.map(u=>`<url><loc>${esc(u)}</loc></url>`).join("")
  }</urlset>\n`;

  // Return files[] for bulk commit (NOTE: we now include the post HTML and the post JSON verbatim)
  const enc = (s) => Buffer.from(s, "utf8").toString("base64");
  return {
    message: `chore: render ${slug}`,
    overwrite: true,
    files: [
      { path: indexRel,              content_type: "text/html",         content_base64: enc(postHtml) },
      { path: jsonRel,               content_type: "application/json",  content_base64: enc(String(postText).endsWith("\n")?postText:postText+"\n") },
      { path: manifestRel,           content_type: "application/json",  content_base64: enc(JSON.stringify(manifest, null, 2) + "\n") },
      { path: "docs/blogs/index.html",  content_type: "text/html",      content_base64: enc(blogsIndexHtml) },
      { path: `docs/blogs/${year}/index.html`, content_type: "text/html", content_base64: enc(yearIndexHtml) },
      { path: rssRel,                content_type: "application/xml",   content_base64: enc(rss) },
      { path: sitemapRel,            content_type: "application/xml",   content_base64: enc(sitemap) }
    ]
  };
}

// Common proxy handler
async function handleItems(req, res, { head = false } = {}) {
  const rel = (req.params[0] || "").trim();
  if (!isAllowedPath(rel)) return res.status(400).end(head ? "" : JSON.stringify({ error: "Bad path" }));

  const cacheKey = rel;
  const now = Date.now();
  const cached = memoryCache.get(cacheKey);
  const isFresh = cached && now - cached.fetchedAt < CACHE_TTL * 1000;
  const ctype = contentTypeFor(rel);

  const headers = {};
  if (cached?.etag) headers["If-None-Match"] = cached.etag;
  if (cached?.lastModified) headers["If-Modified-Since"] = cached.lastModified;

  if (isFresh) {
    res.set("Content-Type", ctype);
    res.set("Cache-Control", `public, max-age=${Math.min(CACHE_TTL, 60)}`);
    if (cached.etag) res.set("ETag", cached.etag);
    if (cached.lastModified) res.set("Last-Modified", cached.lastModified);
    return head ? res.status(200).end() : res.status(200).send(cached.body);
  }

  const url = upstreamUrl(`items/${rel}`);
  try { assertAllowedUpstream(url); } catch (e) { return res.status(e.code || 400).json({ error: e.message }); }

  const upstreamMethod = head ? "HEAD" : "GET";
  const resp = await fetchWithRetry(url, { method: upstreamMethod, headers }, { attempts: 2, timeoutMs = 5000 });

  if (resp.status === 304 && cached) {
    cached.fetchedAt = now;
    res.set("Content-Type", ctype);
    res.set("Cache-Control", `public, max-age=${Math.min(CACHE_TTL, 60)}`);
    if (cached.etag) res.set("ETag", cached.etag);
    if (cached.lastModified) res.set("Last-Modified", cached.lastModified);
    return head ? res.status(200).end() : res.status(200).send(cached.body);
  }

  if (!resp.ok) return res.status(resp.status).end(head ? "" : JSON.stringify({ error: `Upstream ${resp.status}` }));

  const etag = resp.headers.get("etag") || "";
  const lastModified = resp.headers.get("last-modified") || "";

  if (!head) {
    const text = await resp.text();
    memoryCache.set(cacheKey, { body: text, etag, lastModified, fetchedAt: now });
    res.set("Content-Type", ctype);
    res.set("Cache-Control", `public, max-age=${Math.min(CACHE_TTL, 60)}`);
    if (etag) res.set("ETag", etag);
    if (lastModified) res.set("Last-Modified", lastModified);
    return res.status(200).send(text);
  } else {
    if (cached) {
      if (etag) cached.etag = etag;
      if (lastModified) cached.lastModified = lastModified;
      cached.fetchedAt = now;
    }
    res.set("Content-Type", ctype);
    res.set("Cache-Control", `public, max-age=${Math.min(CACHE_TTL, 60)}`);
    if (etag) res.set("ETag", etag);
    if (lastModified) res.set("Last-Modified", lastModified);
    return res.status(200).end();
  }
}

// Middleware
app.use(morgan("combined"));
app.use(cors({ origin: "*" }));
app.options("*", cors()); // handle preflight
app.use(express.json());

// --- HEAD then GET for /items/* ---
app.head("/items/*", async (req, res) => {
  try { await handleItems(req, res, { head: true }); }
  catch (err) { console.error("HEAD proxy error:", err); res.status(500).end(); }
});

app.get("/items/*", async (req, res) => {
  try { await handleItems(req, res, { head: false }); }
  catch (err) { console.error("Proxy error:", err); res.status(500).json({ error: "Proxy failed" }); }
});

// --- GitHub commit endpoint (supports .json + text files) ---
app.post("/items/commit", async (req, res) => {
  try {
    let { path: p, json, message } = req.body;
    if (!p || json === undefined || json === null) return res.status(400).json({ error: "path and json required" });

    let rel = String(p).replace(/^\/+/, "").replace(/^items\//, "");
    if (!isAllowedPath(rel)) return res.status(400).json({ error: "Path not allowed" });

    const isJson = /\.json$/i.test(rel);
    if (!isJson && typeof json !== "string") return res.status(400).json({ error: "For text files, json must be a string body" });

    let contentStr;
    if (isJson) {
      try { contentStr = JSON.stringify(typeof json === "string" ? JSON.parse(json) : json, null, 2) + "\n"; }
      catch (e) { return res.status(400).json({ error: `Invalid JSON payload: ${e.message}` }); }
    } else {
      contentStr = String(json).replace(/\r\n?/g, "\n");
      if (!contentStr.endsWith("\n")) contentStr += "\n";
    }

    const repo = process.env.GITHUB_REPO;
    const branch = process.env.GITHUB_BRANCH || "main";
    const token = process.env.GITHUB_TOKEN;
    if (!repo || !token) return res.status(500).json({ error: "Missing GITHUB_REPO or GITHUB_TOKEN" });

    const repoPath = `items/${rel}`;
    const api = `https://api.github.com/repos/${repo}/contents/${repoPath}`;
    const headers = { Authorization: `Bearer ${token}`, Accept: "application/vnd.github+json", "User-Agent": "crt-items-proxy" };

    let sha;
    const head = await fetchWithRetry(`${api}?ref=${encodeURIComponent(branch)}`, { headers }, { attempts: 2, timeoutMs: 5000 });
    if (head.ok) { const meta = await head.json(); sha = meta.sha; }

    const body = { message: message || `chore: update ${repoPath}`, content: Buffer.from(contentStr, "utf8").toString("base64"), branch };
    if (sha) body.sha = sha;

    const put = await fetchWithRetry(api, { method: "PUT", headers, body: JSON.stringify(body) }, { attempts: 2, timeoutMs: 7000 });
    const result = await put.json();
    if (!put.ok) return res.status(put.status).json(result);

    memoryCache.delete(rel);

    res.json({ ok: true, path: repoPath, commit: result.commit && { sha: result.commit.sha, url: result.commit.html_url } });
  } catch (e) {
    console.error(e);
    res.status(500).json({ error: "Commit failed" });
  }
});

// --- /docs/commit (JSON-only single-file commit)
app.post("/docs/commit", async (req, res) => {
  try {
    let { path: p, json, message } = req.body || {};
    if (!p || json === undefined || json === null) {
      return res.status(400).json({ error: "path and json required" });
    }
    // normalize + enforce docs/
    let rel = String(p).trim().replace(/^\/+/, "").replace(/^docs\//, "");
    if (!/\.json$/i.test(rel)) return res.status(400).json({ error: "Docs commits require a .json file path" });

    const repo   = process.env.GITHUB_REPO;
    const branch = process.env.GITHUB_BRANCH || "main";
    const token  = process.env.GITHUB_TOKEN;
    if (!repo || !token) return res.status(500).json({ error: "Missing GITHUB_REPO or GITHUB_TOKEN" });

    // stringify JSON payload
    let contentStr;
    try { contentStr = JSON.stringify(typeof json === "string" ? JSON.parse(json) : json, null, 2) + "\n"; }
    catch (e) { return res.status(400).json({ error: `Invalid JSON payload: ${e.message}` }); }

    const repoPath = `docs/${rel}`;
    const api = `https://api.github.com/repos/${repo}/contents/${repoPath}`;
    const headers = { Authorization: `Bearer ${token}`, Accept: "application/vnd.github+json", "User-Agent": "crt-docs-proxy" };

    // existing SHA
    let sha;
    const head = await fetchWithRetry(`${api}?ref=${encodeURIComponent(branch)}`, { headers }, { attempts: 2, timeoutMs: 5000 });
    if (head.ok) { const meta = await head.json(); sha = meta.sha; }

    const body = { message: message || `chore: commit ${repoPath}`, content: Buffer.from(contentStr, "utf8").toString("base64"), branch };
    if (sha) body.sha = sha;

    const put = await fetchWithRetry(api, { method: "PUT", headers, body: JSON.stringify(body) }, { attempts: 2, timeoutMs: 7000 });
    const result = await put.json();
    if (!put.ok) return res.status(put.status).json(result);

    memoryCache.delete(`docs/${rel}`);
    return res.status(200).json({ ok: true, path: repoPath, commit: result.commit && { sha: result.commit.sha, url: result.commit.html_url } });
  } catch (e) {
    console.error(e);
    return res.status(500).json({ error: "Docs commit failed" });
  }
});

// --- /docs/commit-bulk (trigger-aware)
app.post("/docs/commit-bulk", async (req, res) => {
  try {
    let { message, overwrite, files, trigger, add_post, canonical_base, images_link, allow_urls } = req.body || {};

    // Accept trigger payload either under `trigger` or top-level fields
    if (!files && (trigger || add_post)) {
      const trig = trigger || { add_post, canonical_base, images_link, allow_urls };
      const built = await buildFilesFromTrigger(trig);
      message = built.message;
      overwrite = built.overwrite;
      files = built.files;
    }

    if (!Array.isArray(files) || files.length === 0) {
      return res.status(400).json({ error: "files[] required or provide trigger.add_post" });
    }

    const repo   = process.env.GITHUB_REPO;
    const branch = process.env.GITHUB_BRANCH || "main";
    const token  = process.env.GITHUB_TOKEN;
    if (!repo || !token) return res.status(500).json({ error: "Missing GITHUB_REPO or GITHUB_TOKEN" });

    // Validate files
    const allowedExt = /\.(html|xml|json)$/i;
    const norm = files.map((f, i) => {
      const p = String(f.path || "").trim().replace(/^\/+/, "");
      if (!p || !p.toLowerCase().startsWith("docs/")) throw new Error(`Bad path at index ${i}`);
      if (p.includes("..")) throw new Error(`Unsafe path at index ${i}`);
      if (!allowedExt.test(p)) throw new Error(`Disallowed extension at index ${i}`);
      const content_base64 = String(f.content_base64 || "");
      if (!content_base64) throw new Error(`Missing content_base64 at index ${i}`);
      return { path: p, b64: content_base64 };
    });

    // GitHub API helpers
    const gh = async (url, opts = {}, attempts = 2, timeoutMs = 7000) => {
      const headers = {
        Authorization: `Bearer ${token}`,
        Accept: "application/vnd.github+json",
        "User-Agent": "crt-docs-proxy",
        ...(opts.headers || {})
      };
      return fetchWithRetry(url, { ...opts, headers }, { attempts, timeoutMs });
    };

    // Resolve refs and base tree
    const refUrl = `https://api.github.com/repos/${repo}/git/refs/heads/${encodeURIComponent(branch)}`;
    const refResp = await gh(refUrl);
    if (!refResp.ok) return res.status(refResp.status).json({ error: `ref ${refResp.status}` });
    const refJson = await refResp.json();
    const baseCommitSha = refJson.object?.sha;
    if (!baseCommitSha) return res.status(500).json({ error: "Missing base commit sha" });

    const commitResp = await gh(`https://api.github.com/repos/${repo}/git/commits/${baseCommitSha}`);
    if (!commitResp.ok) return res.status(commitResp.status).json({ error: `commit ${commitResp.status}` });
    const commitJson = await commitResp.json();
    const baseTreeSha = commitJson.tree?.sha;
    if (!baseTreeSha) return res.status(500).json({ error: "Missing base tree sha" });

    // Create blobs
    const blobShas = [];
    for (const f of norm) {
      const blobResp = await gh(`https://api.github.com/repos/${repo}/git/blobs`, {
        method: "POST",
        body: JSON.stringify({ content: f.b64, encoding: "base64" })
      });
      const blobJson = await blobResp.json();
      if (!blobResp.ok) return res.status(blobResp.status).json({ error: `blob ${blobResp.status}`, details: blobJson });
      blobShas.push({ path: f.path, sha: blobJson.sha });
    }

    // Create tree
    const tree = blobShas.map(x => ({ path: x.path, mode: "100644", type: "blob", sha: x.sha }));
    const treeResp = await gh(`https://api.github.com/repos/${repo}/git/trees`, {
      method: "POST",
      body: JSON.stringify({ base_tree: baseTreeSha, tree })
    });
    const treeJson = await treeResp.json();
    if (!treeResp.ok) return res.status(treeResp.status).json({ error: `tree ${treeResp.status}`, details: treeJson });
    const newTreeSha = treeJson.sha;

    // Create commit
    const msg = message || "chore: docs bulk update";
    const commitPost = await gh(`https://api.github.com/repos/${repo}/git/commits`, {
      method: "POST",
      body: JSON.stringify({ message: msg, tree: newTreeSha, parents: [baseCommitSha] })
    });
    const commitPostJson = await commitPost.json();
    if (!commitPost.ok) return res.status(commitPost.status).json({ error: `commit-post ${commitPost.status}`, details: commitPostJson });
    const newCommitSha = commitPostJson.sha;

    // Update ref
    const refPatch = await gh(refUrl, { method: "PATCH", body: JSON.stringify({ sha: newCommitSha, force: !!overwrite }) });
    const refPatchJson = await refPatch.json();
    if (!refPatch.ok) return res.status(refPatch.status).json({ error: `ref-patch ${refPatch.status}`, details: refPatchJson });

    // Invalidate simple cache entries
    for (const f of norm) {
      memoryCache.delete(f.path);
      memoryCache.delete(f.path.replace(/^docs\//, ""));
    }

    const committed_paths = norm.map(f => f.path);
    const htmlUrl = commitPostJson.html_url || `https://github.com/${repo}/commit/${newCommitSha}`;
    return res.status(200).json({ ok: true, commit: { sha: newCommitSha, url: htmlUrl }, committed_paths });
  } catch (e) {
    console.error(e);
    return res.status(500).json({ error: e.message || "bulk commit failed" });
  }
});

// GET /docs/* -> proxy-read from upstream repo (raw)
app.get("/docs/*", async (req, res) => {
  try {
    // normalize and enforce docs/
    const relParam = String(req.params[0] || "").trim().replace(/^\/+/, "");
    const clean = relParam.replace(/^docs\//, "");
    const repoPath = `docs/${clean}`;

    const upstream = `${UPSTREAM_BASE.replace(/\/+$/, "")}/${repoPath}`;
    try { assertAllowedUpstream(upstream); } catch (e) { return res.status(e.code || 400).json({ error: e.message }); }

    const resp = await fetchWithRetry(upstream, { method: "GET" }, { attempts: 2, timeoutMs: 5000 });
    if (!resp.ok) {
      return res.status(resp.status).json({ error: `Upstream ${resp.status}`, path: repoPath });
    }

    const isJson = /\.json($|\?)/i.test(repoPath);
    res.set("Cache-Control", `public, max-age=${Number(CACHE_TTL) || 0}`);
    res.type(isJson ? "application/json; charset=utf-8" : "text/plain; charset=utf-8");

    const body = await resp.text();
    return res.status(200).send(body);
  } catch (e) {
    console.error(e);
    return res.status(500).json({ error: e.message || String(e) });
  }
});

// boot
const PORT = process.env.PORT || 3000;
app.listen(PORT, () => {
  console.log(`CRT items proxy running on ${PORT}`);
});
