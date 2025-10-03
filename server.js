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
const UPSTREAM_BASE = process.env.UPSTREAM_BASE; // e.g. https://raw.githubusercontent.com/sportdogfood/clear-round-datasets/main/items
if (!UPSTREAM_BASE) throw new Error("Missing UPSTREAM_BASE");

const ALLOW_DIRS = new Set(
  (process.env.ALLOW_DIRS || "")
    .split(",")
    .map(s => s.trim())
    .filter(Boolean)
);

const CACHE_TTL = parseInt(process.env.CACHE_TTL || "300", 10);
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

  const url = upstreamUrl(rel);
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
app.use(morgan("combined"));
app.use(cors({ origin: "*" }));
app.use(express.json());

// --- routes go below this line ---


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



// boot
const PORT = process.env.PORT || 3000;
app.listen(PORT, () => {
  console.log(`CRT items proxy running on ${PORT}`);
});


