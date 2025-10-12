// server.js — minimal proxy + commit APIs (no rendering, no runner logic)

// Core imports
const express = require("express");
const fetch = require("node-fetch");
const cors = require("cors");
const morgan = require("morgan");
require("dotenv").config();

// App
const app = express();

// --- Config via env ---
const UPSTREAM_BASE = process.env.UPSTREAM_BASE; // e.g. https://raw.githubusercontent.com/sportdogfood/clear-round-datasets/main
if (!UPSTREAM_BASE) throw new Error("Missing UPSTREAM_BASE");

// Always keep Airtable allowed in CORS origins (can add more via env)
const ALLOW_ORIGINS = new Set(
  (process.env.ALLOW_ORIGINS ||
    "https://items.clearroundtravel.com,https://blog.clearroundtravel.com,https://airtable.com,https://app.airtable.com,https://console.airtable.com"
  )
    .split(",")
    .map(s => s.trim())
    .filter(Boolean)
);

// Enforce raw host allowlist
const ALLOW_UPSTREAM_HOSTS = new Set(
  (process.env.ALLOW_UPSTREAM_HOSTS || "raw.githubusercontent.com")
    .split(",")
    .map(s => s.trim().toLowerCase())
    .filter(Boolean)
);

// Directories allowed under /items/*
const DEFAULT_ALLOW_DIRS =
  "events,months,seasons,days,years,weeks,labels,places,sources,organizers,cities,countries,hotels,states,weather,airports,venues,restaurants,agents,dine,essentials,legs,distances,insiders,keywords,audience,tone,ratings,links,spots,sections,bullets,services,stay,amenities,slots,cuisines,menus,locale,things,tags,blogs,platforms,geos,timezones,geometry,chains,knowledge,levels,types,core,brand,meta,hubs,zones,seo,outputs,tasks,instructions,schema,gold,policy,docs,runners,images,assets";

const ALLOW_DIRS = new Set(
  (process.env.ALLOW_DIRS || DEFAULT_ALLOW_DIRS)
    .split(",")
    .map(s => s.trim())
    .filter(Boolean)
);

const CACHE_TTL     = parseInt(process.env.CACHE_TTL || "300", 10);
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

// Common handler used by /items/* proxy routes
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

// --- Middleware ---
app.use(morgan("combined"));
app.use(cors({
  origin: (origin, cb) => {
    // allow no-origin (curl/Postman) and any explicitly allowed origins
    if (!origin || ALLOW_ORIGINS.has(origin)) return cb(null, true);
    return cb(null, false);
  }
}));
app.options("*", cors()); // handle preflight
app.use(express.json({ limit: "2mb" })); // JSON bodies

// --- HEAD then GET for /items/* (proxy read)
app.head("/items/*", async (req, res) => {
  try { await handleItems(req, res, { head: true }); }
  catch (err) { console.error("HEAD proxy error:", err); res.status(500).end(); }
});

app.get("/items/*", async (req, res) => {
  try { await handleItems(req, res, { head: false }); }
  catch (err) { console.error("Proxy error:", err); res.status(500).json({ error: "Proxy failed" }); }
});

// --- GitHub commit endpoint (supports .json + text files) ---
// KEEP: Airtable depends on this route
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

    const repo = GITHUB_REPO;
    const branch = GITHUB_BRANCH;
    const token = GITHUB_TOKEN;
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

// --- /docs/commit (JSON-only single-file commit) ---
// KEEP: Airtable + runners depend on this route
app.post("/docs/commit", async (req, res) => {
  try {
    let { path: p, json, message } = req.body || {};
    if (!p || json === undefined || json === null) {
      return res.status(400).json({ error: "path and json required" });
    }
    // normalize + enforce docs/
    let rel = String(p).trim().replace(/^\/+/, "").replace(/^docs\//, "");
    if (!/\.json$/i.test(rel)) return res.status(400).json({ error: "Docs commits require a .json file path" });

    const repo   = GITHUB_REPO;
    const branch = GITHUB_BRANCH;
    const token  = GITHUB_TOKEN;
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

// --- helpers for /docs/commit-bulk validation ---
const REQ_PATTERNS = {
  publish_json:   /^docs\/blogs\/[^/]+-blogs-\d{4}\/[^/]+\/[^/]+-publish-\d{4}-\d{2}-\d{2}\.json$/i,
  post_index:     /^docs\/blogs\/[^/]+-blogs-\d{4}\/[^/]+\/index\.html$/i,
  blogs_index:    /^docs\/blogs\/index\.html$/i,
  year_index:     /^docs\/blogs\/\d{4}\/index\.html$/i,
  rss_xml:        /^docs\/blogs\/rss\.xml$/i,
  sitemap_xml:    /^docs\/sitemap\.xml$/i,
  manifest_json:  /^docs\/blogs\/manifest\.json$/i
};

const MIN_BYTES = {
  publish_json: 100,
  post_index:   200,
  blogs_index:  200,
  year_index:   200,
  rss_xml:      80,
  sitemap_xml:  80,
  manifest_json:50
};

const DENY_BLOG_SRC_RE = /-blog-\d{4}-\d{2}-\d{2}\.json$/i;

function roleForPath(p) {
  for (const [role, rx] of Object.entries(REQ_PATTERNS)) {
    if (rx.test(p)) return role;
  }
  return null;
}

function decodeBase64Strict(b64) {
  const clean = String(b64).trim().replace(/\s+/g, "");
  // Quick sanity: base64 alphabet + padding
  if (!/^[A-Za-z0-9+/]+={0,2}$/.test(clean)) return { ok: false, reason: "invalid_base64_chars", bytes: 0, buf: null };
  try {
    const buf = Buffer.from(clean, "base64");
    // Reject obviously empty decodes
    if (!buf || buf.length === 0) return { ok: false, reason: "decoded_zero_bytes", bytes: 0, buf: null };
    return { ok: true, bytes: buf.length, buf };
  } catch (e) {
    return { ok: false, reason: "base64_decode_error", bytes: 0, buf: null };
  }
}

// --- /docs/commit-bulk (single Git commit for multiple docs/* files; accepts .html, .xml, .json) ---
// KEEP: runners depend on this route; no trigger/render logic here
app.post("/docs/commit-bulk", async (req, res) => {
  try {
    const { message, overwrite, files } = req.body || {};
    if (!Array.isArray(files) || files.length === 0) {
      return res.status(400).json({ error: "files[] required" });
    }

    const repo   = GITHUB_REPO;
    const branch = GITHUB_BRANCH;
    const token  = GITHUB_TOKEN;
    if (!repo || !token) return res.status(500).json({ error: "Missing GITHUB_REPO or GITHUB_TOKEN" });

    // Normalize + basic shape
    const allowedExt = /\.(html|xml|json)$/i;
    const norm = files.map((f, i) => {
      const p = String(f.path || "").trim().replace(/^\/+/, "");
      if (!p || !p.toLowerCase().startsWith("docs/")) throw new Error(`Bad path at index ${i}`);
      if (p.includes("..")) throw new Error(`Unsafe path at index ${i}`);
      if (!allowedExt.test(p)) throw new Error(`Disallowed extension at index ${i}`);
      const content_base64 = String(f.content_base64 || "");
      if (!content_base64) throw new Error(`Missing content_base64 at index ${i}`);
      return { path: p, b64: content_base64, content_type: f.content_type || "" };
    });

    // --- Validation Gate (x-validate parity) ---
    const errors = [];
    const roleHits = {}; // role -> {path, bytes}
    const unexpected = [];

    // Deny any attempt to write source *-blog-YYYY-MM-DD.json
    for (const f of norm) {
      if (DENY_BLOG_SRC_RE.test(f.path)) {
        errors.push({ path: f.path, reason: "write_to_source_blog_json_forbidden" });
      }
    }

    // Decode, size-check, role matching
    for (const f of norm) {
      const dec = decodeBase64Strict(f.b64);
      if (!dec.ok) {
        errors.push({ path: f.path, reason: dec.reason, decoded_bytes: dec.bytes, min_required: 1 });
        continue;
      }
      const role = roleForPath(f.path);
      if (!role) {
        unexpected.push({ path: f.path, reason: "path_not_in_allowlist" });
        continue;
      }
      // Keep the largest (shouldn't duplicate roles, but guard anyway)
      if (!roleHits[role] || dec.bytes > roleHits[role].bytes) {
        roleHits[role] = { path: f.path, bytes: dec.bytes };
      }
    }

    // Required roles presence + min-bytes thresholds
    for (const [role, min] of Object.entries(MIN_BYTES)) {
      if (!roleHits[role]) {
        errors.push({ role, reason: "required_path_missing", expected_pattern: String(REQ_PATTERNS[role]) });
      } else if (roleHits[role].bytes < min) {
        errors.push({ path: roleHits[role].path, reason: "below_min_bytes", decoded_bytes: roleHits[role].bytes, min_required: min });
      }
    }

    // Unexpected paths (not matching any allowed pattern)
    for (const u of unexpected) errors.push(u);

    if (errors.length > 0) {
      return res.status(400).json({
        error: "validation_failed",
        details: errors
      });
    }
    // --- End Validation Gate ---

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
      memoryCache.delete(f.path);                         // exact docs/… key
      memoryCache.delete(f.path.replace(/^docs\//, ""));  // fallback variant
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
  console.log(`[startup] UPSTREAM_BASE='${UPSTREAM_BASE}'`);
  console.log(`[startup] ALLOW_DIRS=${Array.from(ALLOW_DIRS).join(",")}`);
  console.log(`[startup] ALLOW_ORIGINS=${Array.from(ALLOW_ORIGINS).join(",")}`);
  console.log(`[startup] ALLOW_UPSTREAM_HOSTS=${Array.from(ALLOW_UPSTREAM_HOSTS).join(",")}`);
  console.log(`CRT items proxy running on ${PORT}`);
});

