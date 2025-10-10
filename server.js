// server.js — header and helpers (no routes)

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
  (process.env.ALLOW_DIRS || "items,items/runners,items/tasks,items/brand,items/venues,items/events,items/schema,items/gold,items/policy")
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

// Common handler used by routes

// ADD ABOVE the /docs/commit-bulk route (place near other helpers)
async function buildFilesFromTrigger(trigger) {
  // trigger: { add_post, canonical_base, images_link?, allow_urls?[] }
  if (!trigger || !trigger.add_post) {
    const e = new Error("trigger.add_post required"); e.code = 400; throw e;
  }
  const addUrl = String(trigger.add_post).trim();
  if (!/^https:\/\/raw\.githubusercontent\.com\//i.test(addUrl)) {
    const e = new Error("add_post must be raw.githubusercontent.com"); e.code = 400; throw e;
  }

  // Fetch the post JSON
  const postResp = await fetchWithRetry(addUrl, {}, { attempts: 2, timeoutMs: 6000 });
  if (!postResp.ok) { const t = await postResp.text(); const e = new Error(`add_post fetch ${postResp.status}: ${t}`); e.code = 400; throw e; }
  const postJson = await postResp.json();

  // Parse venue/year/slug from filename + parent folder
  // Expect: .../docs/blogs/{venue}-blogs-{year}/{slug}/{slug}.json
  const m = addUrl.match(/\/docs\/blogs\/([^/]+)-blogs-(\d{4})\/([^/]+)\/\3\.json$/i);
  if (!m) { const e = new Error("add_post path does not match expected blog structure"); e.code = 400; throw e; }
  const venue = m[1], year = m[2], slug = m[3];

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
  const title = postJson?.seo?.section_title || postJson?.seo?.open_graph_title || slug;
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

  // Optional images_link for card image
  let card_image = "/assets/images/card-placeholder.jpg";
  if (trigger.images_link) {
    try {
      const imgResp = await fetchWithRetry(trigger.images_link, {}, { attempts: 2, timeoutMs: 5000 });
      if (imgResp.ok) {
        const imgs = await imgResp.json();
        if (imgs?.card_image_link) card_image = imgs.card_image_link;
      }
    } catch {}
  }

  // Upsert manifest entry
  const pathPublic = `/blogs/${venue}-blogs-${year}/${slug}/`;
  const jsonPublic = `/${jsonRel.replace(/^docs\//,"")}`;
  const entry = {
    slug, title, date,
    year, month_num, month_name, season,
    path: pathPublic,
    json: jsonPublic,
    image: card_image,
    venue,
  };
  const idx = manifest.findIndex(x => x.slug === slug);
  if (idx >= 0) manifest[idx] = { ...manifest[idx], ...entry };
  else manifest.push(entry);

  // Minimal index.html placeholder (does NOT overwrite the JSON)
  const html = [
    "<!doctype html>",
    "<html><head>",
    `<meta charset="utf-8"><title>${title}</title>`,
    `<link rel="canonical" href="${canonicalBase}${pathPublic}">`,
    `<meta name="viewport" content="width=device-width,initial-scale=1">`,
    "</head><body>",
    `<main><h1>${title}</h1>`,
    `<p>Post shell for <code>${slug}</code>. Body JSON lives at <a href="/${jsonRel.replace(/^docs\//,"")}">${slug}.json</a>.</p>`,
    `<p>This placeholder exists so Structure Runner templates can hydrate later.</p>`,
    `<nav><a href="/blogs/">All blogs</a></nav>`,
    "</main>",
    "</body></html>\n"
  ].join("");

  // Rebuild RSS and sitemap very simply (placeholders; your full templates will replace)
  const rss = `<?xml version="1.0" encoding="UTF-8"?><rss version="2.0"><channel><title>Blog RSS</title>${manifest
    .slice()
    .sort((a,b)=>String(b.date).localeCompare(a.date))
    .slice(0,20)
    .map(it=>`<item><title>${it.title}</title><link>${canonicalBase}${it.path}</link><pubDate>${it.date}</pubDate></item>`).join("")}</channel></rss>\n`;

  const smUrls = [
    `${canonicalBase}/blogs/`,
    ...manifest.map(it => `${canonicalBase}${it.path}`)
  ];
  const sitemap = `<?xml version="1.0" encoding="UTF-8"?><urlset xmlns="http://www.sitemaps.org/schemas/sitemap/0.9">${smUrls.map(u=>`<url><loc>${u}</loc></url>`).join("")}</urlset>\n`;

  // Return files[] for bulk commit
  const enc = (s) => Buffer.from(s, "utf8").toString("base64");
  return {
    message: `chore: scaffold ${slug}`,
    overwrite: true,
    files: [
      { path: indexRel, content_type: "text/html", content_base64: enc(html) },
      { path: manifestRel, content_type: "application/json", content_base64: enc(JSON.stringify(manifest, null, 2) + "\n") },
      { path: rssRel, content_type: "application/xml", content_base64: enc(rss) },
      { path: sitemapRel, content_type: "application/xml", content_base64: enc(sitemap) }
      // NOTE: we DO NOT include the post JSON; we never overwrite add_post payload.
    ]
  };
}

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
  const resp = await fetchWithRetry(url, { method: upstreamMethod, headers }, { attempts: 2, timeoutMs: 5000 });

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

// Middleware (keep before routes)
// Middleware (keep before routes)
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

// --- /docs/commit (mirror of /items/commit but forces docs/ and JSON only)
// REPLACE the /docs/commit-bulk route with this updated version (lines before/after kept)
app.post("/docs/commit-bulk", async (req, res) => {
  try {
    let { message, overwrite, files, trigger, add_post, canonical_base, images_link, allow_urls } = req.body || {};

    // NEW: accept trigger payload either under `trigger` or top-level fields
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

// --- /docs/commit-bulk (single Git commit for multiple docs/* files; accepts .html, .xml, .json)
app.post("/docs/commit-bulk", async (req, res) => {
  try {
    const { message, overwrite, files } = req.body || {};
    if (!Array.isArray(files) || files.length === 0) {
      return res.status(400).json({ error: "files[] required" });
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
    const tree = blobShas.map(x => ({
      path: x.path,
      mode: "100644",
      type: "blob",
      sha: x.sha
    }));
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
    const refPatch = await gh(refUrl, {
      method: "PATCH",
      body: JSON.stringify({ sha: newCommitSha, force: !!overwrite })
    });
    const refPatchJson = await refPatch.json();
    if (!refPatch.ok) return res.status(refPatch.status).json({ error: `ref-patch ${refPatch.status}`, details: refPatchJson });

    // Invalidate simple cache entries
    for (const f of norm) {
      memoryCache.delete(f.path);          // exact docs/… key
      memoryCache.delete(f.path.replace(/^docs\//, "")); // fallback variant
    }

    const committed_paths = norm.map(f => f.path);
    const htmlUrl = commitPostJson.html_url || `https://github.com/${repo}/commit/${newCommitSha}`;
    return res.status(200).json({
      ok: true,
      commit: { sha: newCommitSha, url: htmlUrl },
      committed_paths
    });
  } catch (e) {
    console.error(e);
    return res.status(500).json({ error: e.message || "bulk commit failed" });
  }
});

// boot
const PORT = process.env.PORT || 3000;
app.listen(PORT, () => {
  console.log(`CRT items proxy running on ${PORT}`);
});
