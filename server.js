// server.js — minimal proxy + commit APIs (older commit routes + alias for /items/)

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

// Always keep Airtable allowed (older behavior: permissive CORS)
const ALLOW_ORIGINS = new Set(
  (process.env.ALLOW_ORIGINS ||
    "https://items.clearroundtravel.com,https://blog.clearroundtravel.com,https://airtable.com,https://app.airtable.com,https://console.airtable.com"
  )
    .split(",")
    .map(s => s.trim())
    .filter(Boolean)
);

// Enforce raw host allowlist (kept)
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

// --- Middleware (older behavior: permissive CORS) ---
app.use(morgan("combined"));
app.use(cors({ origin: "*" }));
app.options("*", cors());
app.use(express.json({ limit: "2mb" }));

// --- HEAD then GET for /items/* (proxy read)
app.head("/items/*", async (req, res) => {
  try { await handleItems(req, res, { head: true }); }
  catch (err) { console.error("HEAD proxy error:", err); res.status(500).end(); }
});

app.get("/items/*", async (req, res) => {
  try { await handleItems(req, res, { head: false }); }
  catch (err) { console.error("Proxy error:", err); res.status(500).json({ error: "Proxy failed" }); }
});

// --- GitHub commit endpoint (RESTORED) ---
app.post("/items/commit", async (req, res) => {
  try {
    let { path, json, message } = req.body;
    if (!path || json === undefined || json === null) {
      return res.status(400).json({ error: "path and json required" });
    }

    let rel = String(path).replace(/^\/+/, "").replace(/^items\//, "");
    if (!isAllowedPath(rel)) return res.status(400).json({ error: "Path not allowed" });

    const isJson = /\.json$/i.test(rel);
    if (!isJson && typeof json !== "string") {
      return res.status(400).json({ error: "For text files, json must be a string body" });
    }

    const repo = GITHUB_REPO;
    const branch = GITHUB_BRANCH;
    const token = GITHUB_TOKEN;
    if (!repo || !token) {
      return res.status(500).json({ error: "Missing GITHUB_REPO or GITHUB_TOKEN" });
    }

    let contentStr;
    if (isJson) {
      try {
        contentStr = JSON.stringify(
          typeof json === "string" ? JSON.parse(json) : json,
          null,
          2
        ) + "\n";
      } catch (e) {
        return res.status(400).json({ error: `Invalid JSON payload: ${e.message}` });
      }
    } else {
      contentStr = String(json).replace(/\r\n?/g, "\n");
      if (!contentStr.endsWith("\n")) contentStr += "\n";
    }

    const repoPath = `items/${rel}`;
    const api = `https://api.github.com/repos/${repo}/contents/${repoPath}`;
    const headers = {
      Authorization: `Bearer ${token}`,
      Accept: "application/vnd.github+json",
      "User-Agent": "crt-items-proxy"
    };

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

// --- Compat alias: POST /items/ and /items -> same as /items/commit (no refactor, exact behavior)
app.post(["/items", "/items/"], async (req, res, next) => {
  req.url = "/items/commit";           // hand off to the existing handler
  next();
}, app._router.stack.find(l => l.route && l.route.path === "/items/commit").route.stack[0].handle);

// --- /docs/commit (JSON-only single-file commit)
app.post("/docs/commit", async (req, res) => {
  try {
    let { path: p, json, message } = req.body || {};
    if (!p || json === undefined || json === null) {
      return res.status(400).json({ error: "path and json required" });
    }
    let rel = String(p).trim().replace(/^\/+/, "").replace(/^docs\//, "");
    if (!/\.json$/i.test(rel)) return res.status(400).json({ error: "Docs commits require a .json file path" });

    const repo   = GITHUB_REPO;
    const branch = GITHUB_BRANCH;
    const token  = GITHUB_TOKEN;
    if (!repo || !token) return res.status(500).json({ error: "Missing GITHUB_REPO or GITHUB_TOKEN" });

    let contentStr;
    try { contentStr = JSON.stringify(typeof json === "string" ? JSON.parse(json) : json, null, 2) + "\n"; }
    catch (e) { return res.status(400).json({ error: `Invalid JSON payload: ${e.message}` }); }

    const repoPath = `docs/${rel}`;
    const api = `https://api.github.com/repos/${repo}/contents/${repoPath}`;
    const headers = { Authorization: `Bearer ${token}`, Accept: "application/vnd.github+json", "User-Agent": "crt-docs-proxy" };

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

// --- /docs/commit-bulk (single commit for multiple docs/* files)
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

    const gh = async (url, opts = {}, attempts = 2, timeoutMs = 7000) => {
      const headers = {
        Authorization: `Bearer ${token}`,
        Accept: "application/vnd.github+json",
        "User-Agent": "crt-docs-proxy",
        ...(opts.headers || {})
      };
      return fetchWithRetry(url, { ...opts, headers }, { attempts, timeoutMs });
    };

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

    const tree = blobShas.map(x => ({ path: x.path, mode: "100644", type: "blob", sha: x.sha }));
    const treeResp = await gh(`https://api.github.com/repos/${repo}/git/trees`, {
      method: "POST",
      body: JSON.stringify({ base_tree: baseTreeSha, tree })
    });
    const treeJson = await treeResp.json();
    if (!treeResp.ok) return res.status(treeResp.status).json({ error: `tree ${treeResp.status}`, details: treeJson });
    const newTreeSha = treeJson.sha;

    const msg = message || "chore: docs bulk update";
    const commitPost = await gh(`https://api.github.com/repos/${repo}/git/commits`, {
      method: "POST",
      body: JSON.stringify({ message: msg, tree: newTreeSha, parents: [baseCommitSha] })
    });
    const commitPostJson = await commitPost.json();
    if (!commitPost.ok) return res.status(commitPost.status).json({ error: `commit-post ${commitPost.status}`, details: commitPostJson });
    const newCommitSha = commitPostJson.sha;

    const refPatch = await gh(refUrl, { method: "PATCH", body: JSON.stringify({ sha: newCommitSha, force: !!overwrite }) });
    const refPatchJson = await refPatch.json();
    if (!refPatch.ok) return res.status(refPatch.status).json({ error: `ref-patch ${refPatch.status}`, details: refPatchJson });

    for (const f of norm) {
      memoryCache.delete(f.path);
      memoryCache.delete(f.path.replace(/^docs\//, ""));
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
// --- /mirror/github (simple proxy to fetch GitHub raw content)
app.get("/mirror/github", async (req, res) => {
  try {
    const target = req.query.url;
    if (!target) {
      return res.status(400).json({ error: "Missing ?url=" });
    }

    // Basic safety
    if (!/^https:\/\/raw\.githubusercontent\.com\//i.test(target)) {
      return res.status(400).json({ error: "Only raw.githubusercontent.com URLs allowed" });
    }

    const r = await fetch(target);
    const text = await r.text();
    const type = r.headers.get("content-type") || "text/plain";

    res.set("Content-Type", type);
    res.status(r.status).send(text);
  } catch (err) {
    console.error("mirror/github error", err);
    res.status(500).json({ error: "mirror fetch failed", details: err.message });
  }
});

// GET /docs/* -> proxy-read from upstream repo (raw)
app.get("/docs/*", async (req, res) => {
  try {
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
