// server.js — full drop with /stash support (CSV/NDJSON enabled)

const express = require("express");
const fetch = require("node-fetch");
const cors = require("cors");
const morgan = require("morgan");

require("dotenv").config();

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

// Optional: where to stash batch artifacts (under items/)
const STASH_PREFIX = process.env.STASH_PREFIX || "items/stash";

// --- Tiny in-memory cache ---
const memoryCache = new Map(); // key => { body, etag, lastModified, fetchedAt }

// --- helpers
// Allow csv/ndjson alongside json/txt/etc
const ALLOWED_FILE_EXT_RE = /\.(json|txt|md|html|js|csv|ndjson)$/i;

function isAllowedPath(rel) {
  if (!rel) return false;
  if (rel.includes("..")) return false;
  // basic shape: dir/dir/file.ext (allow dots in basename)
  if (!/^[a-z0-9][a-z0-9\-_.\/]*\.[a-z0-9]+$/i.test(rel)) return false;
  // extension must be allowed for GETs
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
    // Serve JSON as text for reliable rendering in clients like mine.
    //case ".json":   return "text/plain; charset=utf-8";
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

// Minimal fetch with timeout + single retry for robustness
async function fetchWithRetry(url, options = {}, { attempts = 2, timeoutMs = 5000 } = {}) {
  let lastErr;
  for (let i = 0; i < attempts; i++) {
    const controller = new AbortController();
    const timer = setTimeout(() => controller.abort(), timeoutMs);
    try {
      const resp = await fetch(url, { ...options, signal: controller.signal });
      clearTimeout(timer);
      // Retry on 5xx; pass through others
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

// --- simple filename/path segment sanitizers for stash ---
function safeSeg(s, allowDots = false) {
  const re = allowDots ? /^[a-z0-9._-]+$/i : /^[a-z0-9_-]+$/i;
  return typeof s === "string" && re.test(s) ? s : null;
}

function joinStashPath(folder, base, name) {
  const f = safeSeg(folder) || "misc";
  const b = safeSeg(base) || new Date().toISOString().replace(/[:.]/g, "").slice(0, 15);
  const n = safeSeg(name, /*allowDots*/ true);
  if (!n) return null;
  // ensure we always live under items/
  const rel = `${STASH_PREFIX}/${f}/${b}/${n}`.replace(/^items\//, "items/");
  return rel;
}

async function commitText(rel, bodyText, message) {
  // reuse commit logic to push text-like files to GitHub
  const repo = process.env.GITHUB_REPO;             // e.g. "sportdogfood/clear-round-datasets"
  const branch = process.env.GITHUB_BRANCH || "main";
  const token = process.env.GITHUB_TOKEN;
  if (!repo || !token) {
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
  const api = `https://api.github.com/repos/${repo}/contents/${repoPath}`;
  const headers = {
    Authorization: `Bearer ${token}`,
    Accept: "application/vnd.github+json",
    "User-Agent": "crt-items-proxy"
  };

  // Fetch existing SHA (if any)
  let sha;
  const head = await fetchWithRetry(`${api}?ref=${encodeURIComponent(branch)}`, { headers }, { attempts: 2, timeoutMs: 5000 });
  if (head.ok) {
    const meta = await head.json();
    sha = meta.sha;
  }

  // Normalize newlines for text; keep as-is otherwise
  const contentStr = String(bodyText).replace(/\r\n?/g, "\n");
  const body = {
    message: message || `stash: ${repoPath}`,
    content: Buffer.from(contentStr, "utf8").toString("base64"),
    branch
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

  // Bust proxy cache (GET path key)
  memoryCache.delete(rel);

  return {
    path: repoPath,
    commit: result.commit && { sha: result.commit.sha, url: result.commit.html_url }
  };
}

// middleware
app.use(morgan("combined"));
app.use(cors({ origin: "*" }));
app.use(express.json());

// health
app.get("/health", (_req, res) => {
  res.json({ ok: true, uptime: process.uptime() });
});

// -------- Generated manifest (JSON index of allowed dirs) --------
const RAW_RE = /^https:\/\/raw\.githubusercontent\.com\/([^/]+)\/([^/]+)\/([^/]+)\/(.+)$/;
const m = RAW_RE.exec(UPSTREAM_BASE);
let GH_OWNER, GH_REPO, GH_BRANCH, GH_BASEPATH;

// Always log what we got for UPSTREAM_BASE
console.log(`[startup] UPSTREAM_BASE='${UPSTREAM_BASE}'`);
console.log(`[startup] ALLOW_DIRS=${Array.from(ALLOW_DIRS).join(",")}`);

if (m) {
  [, GH_OWNER, GH_REPO, GH_BRANCH, GH_BASEPATH] = m;
  console.log(`[manifest] enabled for ${GH_OWNER}/${GH_REPO}@${GH_BRANCH} base='${GH_BASEPATH}'`);
} else {
  console.warn("[manifest] disabled: UPSTREAM_BASE is not raw.githubusercontent.com");
}

// List JSON files in a directory via GitHub API (cached for 24h)
async function listDir(dir) {
  if (!GH_OWNER) throw new Error("Manifest unavailable (UPSTREAM_BASE not raw.github...)");
  const key = `__index__/${dir}`;
  const now = Date.now();
  const cached = memoryCache.get(key);
  const TTL_MS = 24 * 60 * 60 * 1000;
  if (cached && now - cached.fetchedAt < TTL_MS) return cached.body;

  const api = `https://api.github.com/repos/${GH_OWNER}/${GH_REPO}/contents/${GH_BASEPATH}/${dir}?ref=${encodeURIComponent(GH_BRANCH)}`;
  const headers = { Accept: "application/vnd.github+json", "User-Agent": "crt-items-proxy" };
  if (process.env.GITHUB_TOKEN) headers.Authorization = `Bearer ${process.env.GITHUB_TOKEN}`;

  const r = await fetchWithRetry(api, { headers }, { attempts: 2, timeoutMs: 5000 });
  if (!r.ok) throw new Error(`GitHub list error ${r.status}: ${await r.text()}`);
  const json = await r.json();

  const files = Array.isArray(json)
    ? json
        .filter(x => x.type === "file" && /\.json$/i.test(x.name)) // index lists JSON files only
        .map(x => x.name.replace(/\.json$/i, ""))
        .sort()
    : [];

  const body = { dir, count: files.length, files };
  memoryCache.set(key, { body, fetchedAt: now });
  return body;
}

// NOTE: moved OFF /items/manifest.json to avoid clashing with real file paths.
// New endpoints: /_manifest.json and /items/_manifest.json
app.get(["/_manifest.json", "/items/_manifest.json"], async (_req, res) => {
  try {
    if (!GH_OWNER) {
      res.status(501);
      res.set("Content-Type", "text/plain; charset=utf-8");
      return res.send(JSON.stringify({ error: "Manifest disabled for non-raw UPSTREAM_BASE" }, null, 2) + "\n");
    }

    const entries = await Promise.all(
      Array.from(ALLOW_DIRS).map(async dir => {
        try {
          const idx = await listDir(dir);
          return [dir, idx];
        } catch {
          return [dir, { dir, count: 0, files: [] }];
        }
      })
    );

    const payload = {
      generated_at: new Date().toISOString(),
      upstream: { owner: GH_OWNER, repo: GH_REPO, branch: GH_BRANCH, basepath: GH_BASEPATH },
      dirs: Object.fromEntries(entries)
    };

    res.set("X-CRT-Manifest", "generated");
    res.set("Content-Type", "text/plain; charset=utf-8");
    return res.status(200).send(JSON.stringify(payload, null, 2) + "\n");
  } catch (e) {
    console.error("Manifest error:", e);
    res.status(500);
    res.set("Content-Type", "text/plain; charset=utf-8");
    return res.send(JSON.stringify({ error: "Manifest failed" }, null, 2) + "\n");
  }
});

// ---------- TXT alias for any upstream file (read-only) ----------
// Usage: /items/txt/<path>.txt  -> fetches upstream <path> without trailing .txt
// Example: /items/txt/agents/manifest.json.txt  -> reads agents/manifest.json
app.get("/items/txt/*", async (req, res) => {
  try {
    const rawPath = (req.params[0] || "").trim();
    if (!rawPath.endsWith(".txt")) {
      return res.status(400).json({ error: "Bad path" });
    }
    const stripped = rawPath.slice(0, -4); // remove trailing .txt
    if (!isAllowedPath(stripped)) {
      return res.status(400).json({ error: "Bad path" });
    }

    const cacheKey = `txt:${stripped}`;
    const now = Date.now();
    const cached = memoryCache.get(cacheKey);
    const isFresh = cached && now - cached.fetchedAt < CACHE_TTL * 1000;

    if (isFresh) {
      res.set("Content-Type", "text/plain; charset=utf-8");
      res.set("Cache-Control", `public, max-age=${Math.min(CACHE_TTL, 60)}`);
      if (cached.etag) res.set("ETag", cached.etag);
      if (cached.lastModified) res.set("Last-Modified", cached.lastModified);
      return res.status(200).send(cached.body);
    }

    const url = upstreamUrl(stripped);
    const resp = await fetchWithRetry(url, {}, { attempts: 2, timeoutMs: 5000 });
    if (!resp.ok) {
      return res.status(resp.status).json({ error: `Upstream ${resp.status}` });
    }
    const text = await resp.text();
    const etag = resp.headers.get("etag") || "";
    const lastModified = resp.headers.get("last-modified") || "";

    memoryCache.set(cacheKey, { body: text, etag, lastModified, fetchedAt: now });

    res.set("Content-Type", "text/plain; charset=utf-8");
    res.set("Cache-Control", `public, max-age=${Math.min(CACHE_TTL, 60)}`);
    if (etag) res.set("ETag", etag);
    if (lastModified) res.set("Last-Modified", lastModified);
    return res.status(200).send(text);
  } catch (err) {
    console.error("TXT alias error:", err);
    res.status(500).json({ error: "Proxy failed" });
  }
});

// proxy: /items/<dir>/<file>.<ext>   (GET supports .json, .txt, .md, .html, .js, .csv, .ndjson)
app.get("/items/*", async (req, res) => {
  try {
    const rel = (req.params[0] || "").trim();
    if (!isAllowedPath(rel)) return res.status(400).json({ error: "Bad path" });

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
      return res.status(200).send(cached.body);
    }

    const url = upstreamUrl(rel);
    const resp = await fetchWithRetry(url, { headers }, { attempts: 2, timeoutMs: 5000 });

    if (resp.status === 304 && cached) {
      cached.fetchedAt = now;
      res.set("Content-Type", ctype);
      res.set("Cache-Control", `public, max-age=${Math.min(CACHE_TTL, 60)}`);
      if (cached.etag) res.set("ETag", cached.etag);
      if (cached.lastModified) res.set("Last-Modified", cached.lastModified);
      return res.status(200).send(cached.body);
    }

    if (!resp.ok) {
      return res.status(resp.status).json({ error: `Upstream ${resp.status}` });
    }

    const text = await resp.text();
    const etag = resp.headers.get("etag") || "";
    const lastModified = resp.headers.get("last-modified") || "";

    memoryCache.set(cacheKey, { body: text, etag, lastModified, fetchedAt: now });

    res.set("Content-Type", ctype);
    res.set("Cache-Control", `public, max-age=${Math.min(CACHE_TTL, 60)}`);
    if (etag) res.set("ETag", etag);
    if (lastModified) res.set("Last-Modified", lastModified);
    return res.status(200).send(text);
  } catch (err) {
    console.error("Proxy error:", err);
    res.status(500).json({ error: "Proxy failed" });
  }
});

// --- GitHub commit endpoint (supports .json + text files) ---
app.post("/items/commit", async (req, res) => {
  try {
    let { path, json, message } = req.body;
    if (!path || json === undefined || json === null) {
      return res.status(400).json({ error: "path and json required" });
    }

    // Normalize path: accept "items/..." or bare "...", strip leading slashes
    let rel = String(path).replace(/^\/+/, "").replace(/^items\//, "");

    // Validate target path (dir+ext must be allowed)
    if (!isAllowedPath(rel)) return res.status(400).json({ error: "Path not allowed" });

    const isJson = /\.json$/i.test(rel);
    // For non-.json files, require the client to send a string (the file body)
    if (!isJson && typeof json !== "string") {
      return res.status(400).json({ error: "For text files, json must be a string body" });
    }

    const repo = process.env.GITHUB_REPO;             // e.g. "sportdogfood/clear-round-datasets"
    const branch = process.env.GITHUB_BRANCH || "main";
    const token = process.env.GITHUB_TOKEN;
    if (!repo || !token) {
      return res.status(500).json({ error: "Missing GITHUB_REPO or GITHUB_TOKEN" });
    }

    // Compute content string based on extension
    let contentStr;
    if (isJson) {
      try {
        // If client provided a string, parse it first so we pretty-print consistently
        contentStr = JSON.stringify(
          typeof json === "string" ? JSON.parse(json) : json,
          null,
          2
        ) + "\n";
      } catch (e) {
        return res.status(400).json({ error: `Invalid JSON payload: ${e.message}` });
      }
    } else {
      // Normalize text files for clean diffs: LF newlines, ensure trailing newline
      contentStr = String(json).replace(/\r\n?/g, "\n");
      if (!contentStr.endsWith("\n")) contentStr += "\n";
    }

    // Prepare GitHub API call
    const repoPath = `items/${rel}`;
    const api = `https://api.github.com/repos/${repo}/contents/${repoPath}`;
    const headers = {
      Authorization: `Bearer ${token}`,
      Accept: "application/vnd.github+json",
      "User-Agent": "crt-items-proxy"
    };

    // Fetch existing SHA (if any)
    let sha;
    const head = await fetchWithRetry(`${api}?ref=${encodeURIComponent(branch)}`, { headers }, { attempts: 2, timeoutMs: 5000 });
    if (head.ok) {
      const meta = await head.json();
      sha = meta.sha;
    }

    const body = {
      message: message || `chore: update ${repoPath}`,
      content: Buffer.from(contentStr, "utf8").toString("base64"),
      branch
    };
    if (sha) body.sha = sha;

    const put = await fetchWithRetry(api, { method: "PUT", headers, body: JSON.stringify(body) }, { attempts: 2, timeoutMs: 7000 });
    const result = await put.json();
    if (!put.ok) return res.status(put.status).json(result);

    // Bust proxy cache (GET path key)
    memoryCache.delete(rel);

    res.json({
      ok: true,
      path: repoPath,
      commit: result.commit && { sha: result.commit.sha, url: result.commit.html_url }
    });
  } catch (e) {
    console.error(e);
    res.status(500).json({ error: "Commit failed" });
  }
});

// ----------------- STASH: accept batch artifacts and persist to GitHub -----------------
app.post("/stash", express.json({ limit: "5mb" }), async (req, res) => {
  try {
    const { folder, filename_base, files } = req.body || {};
    if (!Array.isArray(files) || files.length === 0) {
      return res.status(400).json({ error: "files[] required" });
    }

    const f = safeSeg(folder) || "misc";
    const b = safeSeg(filename_base) || new Date().toISOString().replace(/[:.]/g, "").slice(0, 15);

    const out = [];
    for (const file of files) {
      if (!file || !file.name || file.body === undefined) {
        return res.status(400).json({ error: "each file requires name and body" });
      }
      const rel = joinStashPath(f, b, file.name);
      if (!rel) return res.status(400).json({ error: `bad file name: ${file.name}` });

      // enforce allowed extensions
      if (!ALLOWED_FILE_EXT_RE.test(rel)) {
        return res.status(400).json({ error: `extension not allowed for ${file.name}` });
      }

      const message = `stash(${f}/${b}): ${file.name}`;
      const result = await commitText(rel, String(file.body), message);
      const publicUrl = `${req.protocol}://${req.get("host")}/items/${rel.replace(/^items\//, "")}`;

      out.push({
        name: file.name,
        path: rel,
        url: publicUrl,
        commit: result.commit
      });
    }

    return res.json({ ok: true, folder: f, filename_base: b, files: out });
  } catch (err) {
    console.error("stash error:", err);
    const code = err.code && Number.isInteger(err.code) ? err.code : 500;
    return res.status(code).json({ error: "stash failed", details: err.message || String(err) });
  }
});
// NEW ROUTE: serve essentials / hub-data
app.get('/items/agents/hub-data/:file', (req, res) => {
  try {
    const fileName = req.params.file;
    const filePath = path.join(__dirname, 'items/agents/hub-data', fileName);

    if (fs.existsSync(filePath)) {
      res.setHeader('Content-Type', 'application/json');
      res.sendFile(filePath);
    } else {
      res.status(404).json({ error: 'Not found' });
    }
  } catch (err) {
    console.error(err);
    res.status(500).json({ error: 'Server error' });
  }
});
// boot
const PORT = process.env.PORT || 3000;
app.listen(PORT, () => {
  console.log(`CRT items proxy running on ${PORT}`);
});
// server.js — full drop with HEAD support and /stash (CSV/NDJSON enabled)

const express = require("express");
const fetch = require("node-fetch");
const cors = require("cors");
const morgan = require("morgan");
const path = require("path");
const fs = require("fs");

require("dotenv").config();

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

// --- Tiny in-memory cache ---
const memoryCache = new Map(); // key => { body, etag, lastModified, fetchedAt }

// --- helpers
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

// --- simple filename/path segment sanitizers for stash ---
function safeSeg(s, allowDots = false) {
  const re = allowDots ? /^[a-z0-9._-]+$/i : /^[a-z0-9_-]+$/i;
  return typeof s === "string" && re.test(s) ? s : null;
}

function joinStashPath(folder, base, name) {
  const f = safeSeg(folder) || "misc";
  const b = safeSeg(base) || new Date().toISOString().replace(/[:.]/g, "").slice(0, 15);
  const n = safeSeg(name, /*allowDots*/ true);
  if (!n) return null;
  const rel = `${STASH_PREFIX}/${f}/${b}/${n}`.replace(/^items\//, "items/");
  return rel;
}

async function commitText(rel, bodyText, message) {
  const repo = process.env.GITHUB_REPO;             // e.g. "sportdogfood/clear-round-datasets"
  const branch = process.env.GITHUB_BRANCH || "main";
  const token = process.env.GITHUB_TOKEN;
  if (!repo || !token) {
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
  const api = `https://api.github.com/repos/${repo}/contents/${repoPath}`;
  const headers = {
    Authorization: `Bearer ${token}`,
    Accept: "application/vnd.github+json",
    "User-Agent": "crt-items-proxy"
  };

  // Fetch existing SHA (if any)
  let sha;
  const head = await fetchWithRetry(`${api}?ref=${encodeURIComponent(branch)}`, { headers }, { attempts: 2, timeoutMs: 5000 });
  if (head.ok) {
    const meta = await head.json();
    sha = meta.sha;
  }

  const contentStr = String(bodyText).replace(/\r\n?/g, "\n");
  const body = {
    message: message || `stash: ${repoPath}`,
    content: Buffer.from(contentStr, "utf8").toString("base64"),
    branch
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

  return {
    path: repoPath,
    commit: result.commit && { sha: result.commit.sha, url: result.commit.html_url }
  };
}

// middleware
app.use(morgan("combined"));
app.use(cors({ origin: "*" }));
app.use(express.json());

// health
app.get("/health", (_req, res) => {
  res.json({ ok: true, uptime: process.uptime() });
});

// -------- Generated manifest --------
const RAW_RE = /^https:\/\/raw\.githubusercontent\.com\/([^/]+)\/([^/]+)\/([^/]+)\/(.+)$/;
const m = RAW_RE.exec(UPSTREAM_BASE);
let GH_OWNER, GH_REPO, GH_BRANCH, GH_BASEPATH;

console.log(`[startup] UPSTREAM_BASE='${UPSTREAM_BASE}'`);
console.log(`[startup] ALLOW_DIRS=${Array.from(ALLOW_DIRS).join(",")}`);

if (m) {
  [, GH_OWNER, GH_REPO, GH_BRANCH, GH_BASEPATH] = m;
  console.log(`[manifest] enabled for ${GH_OWNER}/${GH_REPO}@${GH_BRANCH} base='${GH_BASEPATH}'`);
} else {
  console.warn("[manifest] disabled: UPSTREAM_BASE is not raw.github...");
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
  if (process.env.GITHUB_TOKEN) headers.Authorization = `Bearer ${process.env.GITHUB_TOKEN}`;

  const r = await fetchWithRetry(api, { headers }, { attempts: 2, timeoutMs: 5000 });
  if (!r.ok) throw new Error(`GitHub list error ${r.status}: ${await r.text()}`);
  const json = await r.json();

  const files = Array.isArray(json)
    ? json
        .filter(x => x.type === "file" && /\.json$/i.test(x.name))
        .map(x => x.name.replace(/\.json$/i, ""))
        .sort()
    : [];

  const body = { dir, count: files.length, files };
  memoryCache.set(key, { body, fetchedAt: now });
  return body;
}

app.get(["/_manifest.json", "/items/_manifest.json"], async (_req, res) => {
  try {
    if (!GH_OWNER) {
      res.status(501);
      res.set("Content-Type", "application/json; charset=utf-8");
      return res.send(JSON.stringify({ error: "Manifest disabled for non-raw UPSTREAM_BASE" }, null, 2) + "\n");
    }

    const entries = await Promise.all(
      Array.from(ALLOW_DIRS).map(async dir => {
        try {
          const idx = await listDir(dir);
          return [dir, idx];
        } catch {
          return [dir, { dir, count: 0, files: [] }];
        }
      })
    );

    const payload = {
      generated_at: new Date().toISOString(),
      upstream: { owner: GH_OWNER, repo: GH_REPO, branch: GH_BRANCH, basepath: GH_BASEPATH },
      dirs: Object.fromEntries(entries)
    };

    res.set("X-CRT-Manifest", "generated");
    res.set("Content-Type", "application/json; charset=utf-8");
    return res.status(200).send(JSON.stringify(payload, null, 2) + "\n");
  } catch (e) {
    console.error("Manifest error:", e);
    res.status(500);
    res.set("Content-Type", "application/json; charset=utf-8");
    return res.send(JSON.stringify({ error: "Manifest failed" }, null, 2) + "\n");
  }
});

// ---------- TXT alias (read-only) ----------
app.get("/items/txt/*", async (req, res) => {
  try {
    const rawPath = (req.params[0] || "").trim();
    if (!rawPath.endsWith(".txt")) return res.status(400).json({ error: "Bad path" });
    const stripped = rawPath.slice(0, -4);
    if (!isAllowedPath(stripped)) return res.status(400).json({ error: "Bad path" });

    const cacheKey = `txt:${stripped}`;
    const now = Date.now();
    const cached = memoryCache.get(cacheKey);
    const isFresh = cached && now - cached.fetchedAt < CACHE_TTL * 1000;

    if (isFresh) {
      res.set("Content-Type", "text/plain; charset=utf-8");
      res.set("Cache-Control", `public, max-age=${Math.min(CACHE_TTL, 60)}`);
      if (cached.etag) res.set("ETag", cached.etag);
      if (cached.lastModified) res.set("Last-Modified", cached.lastModified);
      return res.status(200).send(cached.body);
    }

    const url = upstreamUrl(stripped);
    const resp = await fetchWithRetry(url, {}, { attempts: 2, timeoutMs: 5000 });
    if (!resp.ok) return res.status(resp.status).json({ error: `Upstream ${resp.status}` });
    const text = await resp.text();
    const etag = resp.headers.get("etag") || "";
    const lastModified = resp.headers.get("last-modified") || "";

    memoryCache.set(cacheKey, { body: text, etag, lastModified, fetchedAt: now });

    res.set("Content-Type", "text/plain; charset=utf-8");
    res.set("Cache-Control", `public, max-age=${Math.min(CACHE_TTL, 60)}`);
    if (etag) res.set("ETag", etag);
    if (lastModified) res.set("Last-Modified", lastModified);
    return res.status(200).send(text);
  } catch (err) {
    console.error("TXT alias error:", err);
    res.status(500).json({ error: "Proxy failed" });
  }
});

