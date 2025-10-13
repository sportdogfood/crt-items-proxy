// server.js — CRT items/docs proxy + GitHub commits (Airtable-safe)
// Notes:
// - Keeps Airtable routes intact: /items/commit (JSON & text), CORS for Airtable
// - Adds /docs/commit-bulk for 7-file commits (html/xml/json)
// - Read passthrough: /items/* and /docs/* via UPSTREAM_BASE
// - Minimal guards; tighten later after go-live

const express = require("express");
const fetch = require("node-fetch");
const cors = require("cors");
const morgan = require("morgan");
require("dotenv").config();

const app = express();

// ---- Config
const UPSTREAM_BASE = process.env.UPSTREAM_BASE; // e.g. https://raw.githubusercontent.com/sportdogfood/clear-round-datasets/main
if (!UPSTREAM_BASE) throw new Error("Missing UPSTREAM_BASE");
const GITHUB_REPO   = process.env.GITHUB_REPO;   // e.g. sportdogfood/clear-round-datasets
const GITHUB_BRANCH = process.env.GITHUB_BRANCH || "main";
const GITHUB_TOKEN  = process.env.GITHUB_TOKEN || "";

const ALLOW_ORIGINS = new Set(
  (process.env.ALLOW_ORIGINS ||
    "https://airtable.com,https://app.airtable.com,https://console.airtable.com,https://blog.clearroundtravel.com"
  ).split(",").map(s => s.trim()).filter(Boolean)
);

// Directories allowed under /items/*
const DEFAULT_ALLOW_DIRS =
  "events,months,seasons,days,years,weeks,labels,places,sources,organizers,cities,countries,hotels,states,weather,airports,venues,restaurants,agents,dine,essentials,legs,distances,insiders,keywords,audience,tone,ratings,links,spots,sections,bullets,services,stay,amenities,slots,cuisines,menus,locale,things,tags,blogs,platforms,geos,timezones,geometry,chains,knowledge,levels,types,core,brand,meta,hubs,zones,seo,outputs,tasks,instructions,schema,gold,policy,docs,runners,images,assets";

const ALLOW_DIRS = new Set(
  (process.env.ALLOW_DIRS || DEFAULT_ALLOW_DIRS)
    .split(",").map(s => s.trim()).filter(Boolean)
);

const CACHE_TTL = parseInt(process.env.CACHE_TTL || "300", 10);

// ---- Cache
const memoryCache = new Map(); // key -> { body, etag, lastModified, fetchedAt }

// ---- Helpers
const ALLOWED_FILE_EXT_RE = /\.(json|txt|md|html|js|csv|ndjson|xml)$/i;
function isAllowedPath(rel) {
  if (!rel) return false;
  if (rel.includes("..")) return false;
  if (!/^[a-z0-9][a-z0-9._/-]*\.[a-z0-9]+$/i.test(rel)) return false;
  if (!ALLOWED_FILE_EXT_RE.test(rel)) return false;
  const top = rel.split("/")[0];
  return ALLOW_DIRS.has(top);
}
function upstreamUrl(rel) {
  return `${UPSTREAM_BASE.replace(/\/+$/, "")}/${rel.replace(/^\/+/, "")}`;
}
function contentTypeFor(rel) {
  const ext = rel.toLowerCase().slice(rel.lastIndexOf("."));
  switch (ext) {
    case ".json": return "application/json; charset=utf-8";
    case ".xml":  return "application/xml; charset=utf-8";
    case ".txt":  return "text/plain; charset=utf-8";
    case ".md":   return "text/markdown; charset=utf-8";
    case ".html": return "text/html; charset=utf-8";
    case ".js":   return "application/javascript; charset=utf-8";
    case ".csv":  return "text/csv; charset=utf-8";
    case ".ndjson": return "application/x-ndjson; charset=utf-8";
    default:      return "application/octet-stream";
  }
}
async function fetchWithRetry(url, options = {}, { attempts = 2, timeoutMs = 5000 } = {}) {
  let lastErr;
  for (let i = 0; i < attempts; i++) {
    const controller = new AbortController();
    const t = setTimeout(() => controller.abort(), timeoutMs);
    try {
      const resp = await fetch(url, { ...options, signal: controller.signal });
      clearTimeout(t);
      if (resp.status >= 500 && i + 1 < attempts) { await new Promise(r => setTimeout(r, 300 * (i + 1))); continue; }
      return resp;
    } catch (e) {
      clearTimeout(t);
      lastErr = e;
      if (i + 1 < attempts) { await new Promise(r => setTimeout(r, 300 * (i + 1))); continue; }
      throw lastErr;
    }
  }
  throw lastErr;
}

// ---- Middleware
app.use(morgan("combined"));
app.use(cors({
  origin: (origin, cb) => {
    if (!origin || ALLOW_ORIGINS.has(origin)) return cb(null, true);
    return cb(null, false);
  }
}));
app.options("*", cors());
app.use(express.json({ limit: "3mb" }));

// ---- Proxy GET (items)
async function handleItems(req, res, { head = false } = {}) {
  const rel = (req.params[0] || "").trim();
  if (!isAllowedPath(rel)) return res.status(400).json({ error: "Bad path" });

  const cacheKey = `items/${rel}`;
  const now = Date.now();
  const cached = memoryCache.get(cacheKey);
  const fresh = cached && now - cached.fetchedAt < CACHE_TTL * 1000;
  const ctype = contentTypeFor(rel);

  if (fresh) {
    res.set("Content-Type", ctype);
    res.set("Cache-Control", `public, max-age=${Math.min(CACHE_TTL, 60)}`);
    if (cached.etag) res.set("ETag", cached.etag);
    if (cached.lastModified) res.set("Last-Modified", cached.lastModified);
    return head ? res.status(200).end() : res.status(200).send(cached.body);
  }

  const url = upstreamUrl(`items/${rel}`);
  const resp = await fetchWithRetry(url, { method: head ? "HEAD" : "GET" }, { attempts: 2, timeoutMs: 5000 });

  if (!resp.ok) return res.status(resp.status).json({ error: `Upstream ${resp.status}` });

  const etag = resp.headers.get("etag") || "";
  const lastModified = resp.headers.get("last-modified") || "";
  if (head) {
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

  const text = await resp.text();
  memoryCache.set(cacheKey, { body: text, etag, lastModified, fetchedAt: now });

  res.set("Content-Type", ctype);
  res.set("Cache-Control", `public, max-age=${Math.min(CACHE_TTL, 60)}`);
  if (etag) res.set("ETag", etag);
  if (lastModified) res.set("Last-Modified", lastModified);
  return res.status(200).send(text);
}
app.head("/items/*", (req, res) => { handleItems(req, res, { head: true }).catch(e => { console.error(e); res.status(500).end(); }); });
app.get ("/items/*", (req, res) => { handleItems(req, res).catch(e => { console.error(e); res.status(500).json({ error: "Proxy failed" }); }); });

// ---- Airtable-friendly commit (items/commit) — JSON or text
app.post("/items/commit", async (req, res) => {
  try {
    let { path, json, message } = req.body || {};
    if (!path || json === undefined || json === null) return res.status(400).json({ error: "path and json required" });

    let rel = String(path).replace(/^\/+/, "").replace(/^items\//, "");
    if (!isAllowedPath(rel)) return res.status(400).json({ error: "Path not allowed" });

    const isJson = /\.json$/i.test(rel);
    let contentStr;
    if (isJson) {
      try { contentStr = JSON.stringify(typeof json === "string" ? JSON.parse(json) : json, null, 2) + "\n"; }
      catch (e) { return res.status(400).json({ error: `Invalid JSON: ${e.message}` }); }
    } else {
      contentStr = String(json).replace(/\r\n?/g, "\n");
      if (!contentStr.endsWith("\n")) contentStr += "\n";
    }

    if (!GITHUB_REPO || !GITHUB_TOKEN) return res.status(500).json({ error: "Missing GITHUB_REPO or GITHUB_TOKEN" });

    const repoPath = `items/${rel}`;
    const api = `https://api.github.com/repos/${GITHUB_REPO}/contents/${repoPath}`;
    const headers = { Authorization: `Bearer ${GITHUB_TOKEN}`, Accept: "application/vnd.github+json", "User-Agent": "crt-items-proxy" };

    let sha;
    const head = await fetchWithRetry(`${api}?ref=${encodeURIComponent(GITHUB_BRANCH)}`, { headers });
    if (head.ok) { const meta = await head.json(); sha = meta.sha; }

    const body = { message: message || `chore: update ${repoPath}`, content: Buffer.from(contentStr, "utf8").toString("base64"), branch: GITHUB_BRANCH };
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

// ---- Compat alias (some Airtable scripts)
app.post("/items/alias/commit", (req, res, next) => {
  req.url = "/items/commit";
  next();
}, (req, res) => {}); // no-op, flows into /items/commit above

// ---- docs/commit (JSON single)
app.post("/docs/commit", async (req, res) => {
  try {
    let { path, json, message } = req.body || {};
    if (!path || json === undefined || json === null) return res.status(400).json({ error: "path and json required" });

    let rel = String(path).trim().replace(/^\/+/, "").replace(/^docs\//, "");
    if (!/\.json$/i.test(rel)) return res.status(400).json({ error: "Docs JSON only" });

    if (!GITHUB_REPO || !GITHUB_TOKEN) return res.status(500).json({ error: "Missing GITHUB_REPO or GITHUB_TOKEN" });

    let contentStr;
    try { contentStr = JSON.stringify(typeof json === "string" ? JSON.parse(json) : json, null, 2) + "\n"; }
    catch (e) { return res.status(400).json({ error: `Invalid JSON: ${e.message}` }); }

    const repoPath = `docs/${rel}`;
    const api = `https://api.github.com/repos/${GITHUB_REPO}/contents/${repoPath}`;
    const headers = { Authorization: `Bearer ${GITHUB_TOKEN}`, Accept: "application/vnd.github+json", "User-Agent": "crt-docs-proxy" };

    let sha;
    const head = await fetchWithRetry(`${api}?ref=${encodeURIComponent(GITHUB_BRANCH)}`, { headers });
    if (head.ok) { const meta = await head.json(); sha = meta.sha; }

    const body = { message: message || `chore: commit ${repoPath}`, content: Buffer.from(contentStr, "utf8").toString("base64"), branch: GITHUB_BRANCH };
    if (sha) body.sha = sha;

    const put = await fetchWithRetry(api, { method: "PUT", headers, body: JSON.stringify(body) }, { attempts: 2, timeoutMs: 7000 });
    const result = await put.json();
    if (!put.ok) return res.status(put.status).json(result);

    memoryCache.delete(`docs/${rel}`);
    res.json({ ok: true, path: repoPath, commit: result.commit && { sha: result.commit.sha, url: result.commit.html_url } });
  } catch (e) {
    console.error(e);
    res.status(500).json({ error: "Docs commit failed" });
  }
});

// ---- docs/commit-bulk (html/xml/json)
app.post("/docs/commit-bulk", async (req, res) => {
  try {
    const { message, overwrite, files } = req.body || {};
    if (!Array.isArray(files) || files.length === 0) return res.status(400).json({ error: "files[] required" });

    if (!GITHUB_REPO || !GITHUB_TOKEN) return res.status(500).json({ error: "Missing GITHUB_REPO or GITHUB_TOKEN" });

    const norm = files.map((f, i) => {
      const p = String(f.path || "").trim().replace(/^\/+/, "");
      if (!p.toLowerCase().startsWith("docs/")) throw new Error(`Bad path at index ${i}`);
      if (p.includes("..")) throw new Error(`Unsafe path at index ${i}`);
      if (!/\.(json|xml|html)$/i.test(p)) throw new Error(`Disallowed extension at index ${i}`);
      const b64 = String(f.content_base64 || "");
      if (!b64) throw new Error(`Missing content_base64 at index ${i}`);
      return { path: p, b64, ct: f.content_type || (p.endsWith(".json") ? "application/json" : p.endsWith(".xml") ? "application/xml" : "text/html") };
    });

    // GitHub: create blobs -> tree -> commit -> update ref
    const gh = async (url, opts = {}, attempts = 2, timeoutMs = 8000) => {
      const headers = { Authorization: `Bearer ${GITHUB_TOKEN}`, Accept: "application/vnd.github+json", "User-Agent": "crt-docs-proxy", ...(opts.headers || {}) };
      return fetchWithRetry(url, { ...opts, headers }, { attempts, timeoutMs });
    };

    const refUrl = `https://api.github.com/repos/${GITHUB_REPO}/git/refs/heads/${encodeURIComponent(GITHUB_BRANCH)}`;
    const ref = await gh(refUrl);
    if (!ref.ok) return res.status(ref.status).json({ error: `ref ${ref.status}` });
    const refJson = await ref.json();
    const baseCommitSha = refJson.object?.sha;
    if (!baseCommitSha) return res.status(500).json({ error: "Missing base commit sha" });

    const baseCommit = await gh(`https://api.github.com/repos/${GITHUB_REPO}/git/commits/${baseCommitSha}`);
    if (!baseCommit.ok) return res.status(baseCommit.status).json({ error: `commit ${baseCommit.status}` });
    const baseCommitJson = await baseCommit.json();
    const baseTreeSha = baseCommitJson.tree?.sha;

    const blobShas = [];
    for (const f of norm) {
      const r = await gh(`https://api.github.com/repos/${GITHUB_REPO}/git/blobs`, { method: "POST", body: JSON.stringify({ content: f.b64, encoding: "base64" }) });
      const j = await r.json();
      if (!r.ok) return res.status(r.status).json({ error: `blob ${r.status}`, details: j });
      blobShas.push({ path: f.path, sha: j.sha });
    }

    const treeResp = await gh(`https://api.github.com/repos/${GITHUB_REPO}/git/trees`, { method: "POST", body: JSON.stringify({ base_tree: baseTreeSha, tree: blobShas.map(x => ({ path: x.path, mode: "100644", type: "blob", sha: x.sha })) }) });
    const treeJson = await treeResp.json();
    if (!treeResp.ok) return res.status(treeResp.status).json({ error: `tree ${treeResp.status}`, details: treeJson });

    const msg = message || "chore: docs bulk update";
    const commitPost = await gh(`https://api.github.com/repos/${GITHUB_REPO}/git/commits`, { method: "POST", body: JSON.stringify({ message: msg, tree: treeJson.sha, parents: [baseCommitSha] }) });
    const commitPostJson = await commitPost.json();
    if (!commitPost.ok) return res.status(commitPost.status).json({ error: `commit-post ${commitPost.status}`, details: commitPostJson });

    const refPatch = await gh(refUrl, { method: "PATCH", body: JSON.stringify({ sha: commitPostJson.sha, force: !!overwrite }) });
    const refPatchJson = await refPatch.json();
    if (!refPatch.ok) return res.status(refPatch.status).json({ error: `ref-patch ${refPatch.status}`, details: refPatchJson });

    // Invalidate cache
    for (const f of norm) {
      memoryCache.delete(f.path);
      memoryCache.delete(f.path.replace(/^docs\//, ""));
    }

    const committed_paths = norm.map(f => f.path);
    const htmlUrl = commitPostJson.html_url || `https://github.com/${GITHUB_REPO}/commit/${commitPostJson.sha}`;
    res.status(200).json({ ok: true, commit: { sha: commitPostJson.sha, url: htmlUrl }, committed_paths });
  } catch (e) {
    console.error(e);
    res.status(500).json({ error: e.message || "bulk commit failed" });
  }
});

// ---- docs GET passthrough
app.get("/docs/*", async (req, res) => {
  try {
    const relParam = String(req.params[0] || "").trim().replace(/^\/+/, "");
    const repoPath = `docs/${relParam}`;
    const upstream = upstreamUrl(repoPath);
    const resp = await fetchWithRetry(upstream, { method: "GET" });
    if (!resp.ok) return res.status(resp.status).json({ error: `Upstream ${resp.status}`, path: repoPath });
    const body = await resp.text();
    const isJson = /\.json($|\?)/i.test(repoPath);
    res.set("Cache-Control", `public, max-age=${Number(CACHE_TTL) || 0}`);
    res.type(isJson ? "application/json; charset=utf-8" : "text/plain; charset=utf-8");
    res.status(200).send(body);
  } catch (e) {
    console.error(e);
    res.status(500).json({ error: e.message || String(e) });
  }
});

// ---- Boot
const PORT = process.env.PORT || 3000;
app.listen(PORT, () => {
  console.log(`[startup] UPSTREAM_BASE='${UPSTREAM_BASE}'`);
  console.log(`[startup] ALLOW_DIRS=${Array.from(ALLOW_DIRS).join(",")}`);
  console.log(`[startup] ALLOW_ORIGINS=${Array.from(ALLOW_ORIGINS).join(",")}`);
  console.log(`CRT proxy running on ${PORT}`);
});
