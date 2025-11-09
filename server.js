// server.js — Clean CRT Items/Docs Proxy (patched)
// Version 2025-10-14.clean-1c

import express from "express";
import fetch from "node-fetch";
import cors from "cors";
import morgan from "morgan";
import dotenv from "dotenv";
dotenv.config();

const app = express();

// --- Environment setup ---
const UPSTREAM_BASE = process.env.UPSTREAM_BASE; // e.g. https://raw.githubusercontent.com/sportdogfood/clear-round-datasets/main
if (!UPSTREAM_BASE) throw new Error("Missing UPSTREAM_BASE");

const GITHUB_REPO   = process.env.GITHUB_REPO;
const GITHUB_BRANCH = process.env.GITHUB_BRANCH || "main";
const GITHUB_TOKEN  = process.env.GITHUB_TOKEN || "";

const CACHE_TTL = parseInt(process.env.CACHE_TTL || "300", 10);

// tiny in-memory cache
const memoryCache = new Map(); // key -> { body, type, time }

// --- Middleware ---
app.use(morgan("tiny"));
app.use(cors({ origin: "*" }));
app.use(express.json({ limit: "2mb" }));

// PATCH: tolerate double-encoded JSON bodies and stringified "json" field
app.use((req, _res, next) => {
  if (req.is("application/json") && typeof req.body === "string") {
    try { req.body = JSON.parse(req.body); } catch { /* ignore */ }
  }
  // If body exists and has a stringified `json` field, normalize it
  if (req.body && typeof req.body.json === "string") {
    try { req.body.json = JSON.parse(req.body.json); } catch { /* leave as-is */ }
  }
  next();
});

// --- Helpers ---
const allowedExt = /\.(json|txt|html|xml|csv|ndjson|md|tmpl|css)$/i;

const safePath = (rel) =>
  typeof rel === "string" &&
  rel.length > 0 &&
  /^[a-z0-9][\w\-./]+$/i.test(rel) &&
  !rel.includes("..") &&
  allowedExt.test(rel);

// PATCH: robust fetch with retry + timeout + body passthrough
async function fetchWithRetry(
  url,
  options = {},
  retry = { attempts: 2, timeoutMs: 10000 }
) {
  let lastErr;
  for (let i = 0; i < (retry.attempts ?? 2); i++) {
    const controller = new AbortController();
    const timer = setTimeout(() => controller.abort(), retry.timeoutMs ?? 10000);
    try {
      const r = await fetch(url, { ...options, signal: controller.signal });
      clearTimeout(timer);
      // retry only on 5xx
      if (r.status >= 500 && r.status < 600 && i + 1 < (retry.attempts ?? 2)) {
        await new Promise((res) => setTimeout(res, 250 * (i + 1)));
        continue;
      }
      return r;
    } catch (e) {
      clearTimeout(timer);
      lastErr = e;
      if (i + 1 < (retry.attempts ?? 2)) {
        await new Promise((res) => setTimeout(res, 250 * (i + 1)));
        continue;
      }
      throw lastErr;
    }
  }
  throw lastErr;
}

function cacheGet(key) {
  const c = memoryCache.get(key);
  if (!c) return null;
  if (Date.now() - c.time > CACHE_TTL * 1000) {
    memoryCache.delete(key);
    return null;
  }
  return c;
}

function cacheSet(key, body, type) {
  memoryCache.set(key, { body, type, time: Date.now() });
}

// --- /health ---
app.get("/health", (_req, res) => res.status(200).send("OK"));

// --- /items/* — Proxy GET from GitHub main/items ---
app.get("/items/*", async (req, res) => {
  const rel = String(req.params[0] || "");
  if (!safePath(rel)) return res.status(400).json({ error: "Invalid path" });

  const key = `items/${rel}`;
  const cached = cacheGet(key);
  if (cached) {
    res.type(cached.type || "application/json").send(cached.body);
    return;
  }

  const url = `${UPSTREAM_BASE}/items/${rel}`;
  const r = await fetchWithRetry(url, { method: "GET" });
  if (!r.ok) return res.status(r.status).json({ error: `Upstream ${r.status}` });

  const text = await r.text();
  const type = r.headers.get("content-type") || "application/json; charset=utf-8";
  cacheSet(key, text, type);
  res.type(type).send(text);
});

// --- /docs/* — read-only proxy for published outputs ---
app.get("/docs/*", async (req, res) => {
  const rel = String(req.params[0] || "");
  if (!safePath(rel)) return res.status(400).json({ error: "Invalid path" });

  const key = `docs/${rel}`;
  const cached = cacheGet(key);
  if (cached) {
    res.type(cached.type || "application/json").send(cached.body);
    return;
  }

  const url = `${UPSTREAM_BASE}/docs/${rel}`;
  const r = await fetchWithRetry(url, { method: "GET" });
  if (!r.ok) return res.status(r.status).json({ error: `Upstream ${r.status}` });

  const text = await r.text();
  const type = r.headers.get("content-type") || "application/json; charset=utf-8";
  cacheSet(key, text, type);
  res.type(type).send(text);
});

// --- /items/commit — GitHub commit endpoint (single file) ---

// --- GitHub commit endpoint (fixed charset + validation + cache bust + defensive parse) ---
app.post("/items/commit", async (req, res) => {
  try {
    // defensively handle double-stringified bodies from Airtable
    let body = req.body;
    if (typeof body === "string") {
      try { body = JSON.parse(body); } catch { /* leave as-is */ }
    }
    let { path, json, message } = body || {};
    if (!path || json === undefined || json === null) {
      return res.status(400).json({ error: "path and json required" });
    }

    let rel = String(path).replace(/^\/+/, "").replace(/^items\//, "");
    if (!safePath(rel)) return res.status(400).json({ error: "Path not allowed" });

    const isJson = /\.json$/i.test(rel);
    if (!isJson && typeof json !== "string") {
      return res.status(400).json({ error: "For text files, json must be a string body" });
    }

    const repo   = GITHUB_REPO;
    const branch = GITHUB_BRANCH;
    const token  = GITHUB_TOKEN;
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
      "Content-Type": "application/json; charset=utf-8",
      "User-Agent": "crt-items-proxy"
    };

    // HEAD to see if file exists (to include sha)
    let sha;
    const head = await fetchWithRetry(`${api}?ref=${encodeURIComponent(branch)}`, { headers }, { attempts: 2, timeoutMs: 5000 });
    if (head.ok) { const meta = await head.json(); sha = meta.sha; }

    const bodyOut = {
      message: message || `chore: update ${repoPath}`,
      content: Buffer.from(contentStr, "utf8").toString("base64"),
      branch
    };
    if (sha) bodyOut.sha = sha;

    const put = await fetchWithRetry(api, { method: "PUT", headers, body: JSON.stringify(bodyOut) }, { attempts: 2, timeoutMs: 7000 });
    const result = await put.json();
    if (!put.ok) return res.status(put.status).json(result);

    // ✅ Correct cache bust: clear the key used by GET /items/*
    memoryCache.delete(`items/${rel}`);

    return res.json({
      ok: true,
      path: repoPath,
      commit: result.commit && { sha: result.commit.sha, url: result.commit.html_url }
    });
  } catch (e) {
    console.error(e);
    return res.status(500).json({ error: "Commit failed", details: e.message });
  }
});

// --- /docs/commit — single JSON doc commit ---
app.post("/docs/commit", async (req, res) => {
  try {
    let { path: p, json, message } = req.body || {};
    if (!p || json === undefined || json === null) {
      return res.status(400).json({ error: "path and json required" });
    }

    let rel = String(p).trim().replace(/^\/+/, "").replace(/^docs\//i, "");
    // must be .json and must be safe
    if (!/\.json$/i.test(rel) || !safePath(rel)) {
      return res.status(400).json({ error: "Docs commits require a safe .json file path" });
    }

    const repo  = GITHUB_REPO;
    const token = GITHUB_TOKEN;
    const branch = GITHUB_BRANCH;
    if (!repo || !token) return res.status(500).json({ error: "Missing GITHUB_REPO or GITHUB_TOKEN" });

    let contentStr;
    try {
      const obj = (typeof json === "string") ? JSON.parse(json) : json;
      contentStr = JSON.stringify(obj, null, 2) + "\n";
    } catch (e) {
      return res.status(400).json({ error: `Invalid JSON payload: ${e.message}` });
    }

    const repoPath = `docs/${rel}`;
    const api = `https://api.github.com/repos/${repo}/contents/${repoPath}`;
    const headers = {
      Authorization: `Bearer ${token}`,
      Accept: "application/vnd.github+json",
      "Content-Type": "application/json; charset=utf-8",
      "User-Agent": "crt-docs-proxy"
    };

    // HEAD meta (get current sha if exists)
    let sha;
    {
      const head = await fetchWithRetry(`${api}?ref=${encodeURIComponent(branch)}`, { headers }, { attempts: 2, timeoutMs: 5000 });
      if (head.ok) {
        const meta = await head.json();
        sha = meta?.sha;
      }
    }

    const body = {
      message: message || `chore: commit ${repoPath}`,
      content: Buffer.from(contentStr, "utf8").toString("base64"),
      branch
    };
    if (sha) body.sha = sha;

    const put = await fetchWithRetry(
      api,
      { method: "PUT", headers, body: JSON.stringify(body) },
      { attempts: 2, timeoutMs: 10000 }
    );
    const result = await put.json();
    if (!put.ok) return res.status(put.status).json(result);

    memoryCache.delete(repoPath);
    memoryCache.delete(rel);
    memoryCache.delete(`docs/${rel}`);

    return res.status(200).json({
      ok: true,
      path: repoPath,
      commit: result.commit && { sha: result.commit.sha, url: result.commit.html_url }
    });
  } catch (e) {
    console.error("docs/commit error:", e);
    return res.status(500).json({ error: "Docs commit failed", details: e.message });
  }
});

// --- /docs/commit-bulk — Structure Runner (7-file blog publish) ---
app.post("/docs/commit-bulk", async (req, res) => {
  try {
    const { message, overwrite, files } = req.body || {};
    if (!Array.isArray(files) || files.length === 0)
      return res.status(400).json({ error: "files[] required" });

    const repo  = GITHUB_REPO;
    const token = GITHUB_TOKEN;
    const branch = GITHUB_BRANCH;
    if (!repo || !token) return res.status(500).json({ error: "Missing GITHUB_REPO or GITHUB_TOKEN" });

    const headers = {
      Authorization: `Bearer ${token}`,
      Accept: "application/vnd.github+json",
      "User-Agent": "crt-docs-proxy"
    };

    // PATCH: validate each file path and extension and ensure docs/ prefix
    const allowedBulkExt = /\.(html|xml|json)$/i;
    for (const [i, f] of files.entries()) {
      if (!f || !f.path || !f.content_base64) {
        return res.status(400).json({ error: `files[${i}] requires path and content_base64` });
      }
      const p = String(f.path);
      if (!/^docs\//i.test(p)) {
        return res.status(400).json({ error: `files[${i}].path must start with 'docs/'` });
      }
      const clean = p.replace(/^docs\//i, "");
      if (!safePath(clean) || !allowedBulkExt.test(p)) {
        return res.status(400).json({ error: `files[${i}].path is not safe or has disallowed extension` });
      }
    }

    // get current branch ref
    const refUrl = `https://api.github.com/repos/${repo}/git/refs/heads/${encodeURIComponent(branch)}`;
    const ref = await fetchWithRetry(refUrl, { headers }, { attempts: 2, timeoutMs: 7000 });
    if (!ref.ok) return res.status(ref.status).json({ error: `ref ${ref.status}` });
    const refJson = await ref.json();
    const baseCommit = refJson.object?.sha;
    if (!baseCommit) return res.status(500).json({ error: "Missing base commit sha" });

    // find base tree
    const commitResp = await fetchWithRetry(
      `https://api.github.com/repos/${repo}/git/commits/${baseCommit}`, { headers }, { attempts: 2, timeoutMs: 7000 }
    );
    if (!commitResp.ok) return res.status(commitResp.status).json({ error: `commit ${commitResp.status}` });
    const commitJson = await commitResp.json();
    const baseTree = commitJson.tree?.sha;
    if (!baseTree) return res.status(500).json({ error: "Missing base tree sha" });

    // create blobs
    const blobShas = [];
    for (const f of files) {
      const blob = await fetchWithRetry(
        `https://api.github.com/repos/${repo}/git/blobs`,
        {
          method: "POST",
          headers,
          body: JSON.stringify({ content: f.content_base64, encoding: "base64" })
        },
        { attempts: 2, timeoutMs: 7000 }
      );
      const j = await blob.json();
      if (!blob.ok) return res.status(blob.status).json({ error: `blob ${blob.status}`, details: j });
      blobShas.push({ path: f.path, sha: j.sha });
    }

    // create tree
    const treeResp = await fetchWithRetry(
      `https://api.github.com/repos/${repo}/git/trees`,
      {
        method: "POST",
        headers,
        body: JSON.stringify({
          base_tree: baseTree,
          tree: blobShas.map(x => ({ path: x.path, mode: "100644", type: "blob", sha: x.sha }))
        })
      },
      { attempts: 2, timeoutMs: 7000 }
    );
    const treeJson = await treeResp.json();
    if (!treeResp.ok) return res.status(treeResp.status).json({ error: `tree ${treeResp.status}`, details: treeJson });
    const newTree = treeJson.sha;

    // create commit
    const commitPost = await fetchWithRetry(
      `https://api.github.com/repos/${repo}/git/commits`,
      {
        method: "POST",
        headers,
        body: JSON.stringify({ message: message || "bulk publish", tree: newTree, parents: [baseCommit] })
      },
      { attempts: 2, timeoutMs: 7000 }
    );
    const newCommit = await commitPost.json();
    if (!commitPost.ok) return res.status(commitPost.status).json({ error: `commit-post ${commitPost.status}`, details: newCommit });

    // move ref
    const refPatch = await fetchWithRetry(
      refUrl,
      { method: "PATCH", headers, body: JSON.stringify({ sha: newCommit.sha, force: !!overwrite }) },
      { attempts: 2, timeoutMs: 7000 }
    );
    if (!refPatch.ok) {
      const j = await refPatch.json().catch(() => ({}));
      return res.status(refPatch.status).json({ error: `ref-patch ${refPatch.status}`, details: j });
    }

    // invalidate simple caches
    for (const f of files) {
      memoryCache.delete(f.path);
      if (f.path.startsWith("docs/")) {
        memoryCache.delete(f.path.slice(5));
        memoryCache.delete(`docs/${f.path.slice(5)}`);
      }
    }

    res.json({
      ok: true,
      commit: { sha: newCommit.sha, url: newCommit.html_url || `https://github.com/${repo}/commit/${newCommit.sha}` },
      committed_paths: files.map(f => f.path)
    });
  } catch (e) {
    console.error("docs/commit-bulk error:", e);
    res.status(500).json({ error: e.message });
  }
});
// --- /items/commit-bulk — Bulk commit to items/* (runner writes data assets) ---
app.post("/items/commit-bulk", async (req, res) => {
  try {
    const { message, overwrite, files } = req.body || {};
    if (!Array.isArray(files) || files.length === 0)
      return res.status(400).json({ error: "files[] required" });

    const repo  = GITHUB_REPO;
    const token = GITHUB_TOKEN;
    const branch = GITHUB_BRANCH;
    if (!repo || !token) return res.status(500).json({ error: "Missing GITHUB_REPO or GITHUB_TOKEN" });

    const headers = {
      Authorization: `Bearer ${token}`,
      Accept: "application/vnd.github+json",
      "User-Agent": "crt-items-proxy"
    };

    // Validate: enforce items/ prefix and restrict to data-friendly extensions
    const allowedBulkExt = /\.(json|txt|csv|ndjson|md|tmpl)$/i;
    for (const [i, f] of files.entries()) {
      if (!f || !f.path || !f.content_base64) {
        return res.status(400).json({ error: `files[${i}] requires path and content_base64` });
      }
      const p = String(f.path);
      if (!/^items\//i.test(p)) {
        return res.status(400).json({ error: `files[${i}].path must start with 'items/'` });
      }
      const clean = p.replace(/^items\//i, "");
      if (!safePath(clean) || !allowedBulkExt.test(p)) {
        return res.status(400).json({ error: `files[${i}].path is not safe or has disallowed extension` });
      }
    }

    // Get current branch ref
    const refUrl = `https://api.github.com/repos/${repo}/git/refs/heads/${encodeURIComponent(branch)}`;
    const ref = await fetchWithRetry(refUrl, { headers }, { attempts: 2, timeoutMs: 7000 });
    if (!ref.ok) return res.status(ref.status).json({ error: `ref ${ref.status}` });
    const refJson = await ref.json();
    const baseCommit = refJson.object?.sha;
    if (!baseCommit) return res.status(500).json({ error: "Missing base commit sha" });

    // Base tree
    const commitResp = await fetchWithRetry(
      `https://api.github.com/repos/${repo}/git/commits/${baseCommit}`, { headers }, { attempts: 2, timeoutMs: 7000 }
    );
    if (!commitResp.ok) return res.status(commitResp.status).json({ error: `commit ${commitResp.status}` });
    const commitJson = await commitResp.json();
    const baseTree = commitJson.tree?.sha;
    if (!baseTree) return res.status(500).json({ error: "Missing base tree sha" });

    // Create blobs
    const blobShas = [];
    for (const f of files) {
      const blob = await fetchWithRetry(
        `https://api.github.com/repos/${repo}/git/blobs`,
        { method: "POST", headers, body: JSON.stringify({ content: f.content_base64, encoding: "base64" }) },
        { attempts: 2, timeoutMs: 7000 }
      );
      const j = await blob.json();
      if (!blob.ok) return res.status(blob.status).json({ error: `blob ${blob.status}`, details: j });
      blobShas.push({ path: f.path, sha: j.sha });
    }

    // Create tree
    const treeResp = await fetchWithRetry(
      `https://api.github.com/repos/${repo}/git/trees`,
      {
        method: "POST",
        headers,
        body: JSON.stringify({
          base_tree: baseTree,
          tree: blobShas.map(x => ({ path: x.path, mode: "100644", type: "blob", sha: x.sha }))
        })
      },
      { attempts: 2, timeoutMs: 7000 }
    );
    const treeJson = await treeResp.json();
    if (!treeResp.ok) return res.status(treeResp.status).json({ error: `tree ${treeResp.status}`, details: treeJson });
    const newTree = treeJson.sha;

    // Create commit
    const commitPost = await fetchWithRetry(
      `https://api.github.com/repos/${repo}/git/commits`,
      {
        method: "POST",
        headers,
        body: JSON.stringify({ message: message || "items bulk commit", tree: newTree, parents: [baseCommit] })
      },
      { attempts: 2, timeoutMs: 7000 }
    );
    const newCommit = await commitPost.json();
    if (!commitPost.ok) return res.status(commitPost.status).json({ error: `commit-post ${commitPost.status}`, details: newCommit });

    // Move ref
    const refPatch = await fetchWithRetry(
      refUrl,
      { method: "PATCH", headers, body: JSON.stringify({ sha: newCommit.sha, force: !!overwrite }) },
      { attempts: 2, timeoutMs: 7000 }
    );
    if (!refPatch.ok) {
      const j = await refPatch.json().catch(() => ({}));
      return res.status(refPatch.status).json({ error: `ref-patch ${refPatch.status}`, details: j });
    }

    // Invalidate caches for affected items/*
    for (const f of files) {
      memoryCache.delete(f.path); // exact key
      if (f.path.startsWith("items/")) {
        const rel = f.path.slice(6);
        memoryCache.delete(`items/${rel}`); // GET /items/* cache key
      }
    }

    res.json({
      ok: true,
      commit: { sha: newCommit.sha, url: newCommit.html_url || `https://github.com/${repo}/commit/${newCommit.sha}` },
      committed_paths: files.map(f => f.path)
    });
  } catch (e) {
    console.error("items/commit-bulk error:", e);
    res.status(500).json({ error: e.message });
  }
});
// --- /items/commit-simple — lightweight trigger for mobile or voice ---
app.all("/items/commit-simple", async (req, res) => {
  try {
    const { path, message = "simple update" } =
      req.method === "GET" ? req.query : req.body;
    if (!path) return res.status(400).json({ ok: false, error: "missing path" });

    // reuse local proxy instead of direct GitHub write
    const getUrl = `${req.protocol}://${req.get("host")}/items/${path.replace(/^items\//, "")}`;
    const current = await fetchWithRetry(getUrl, { method: "GET" });
    if (!current.ok)
      return res.status(current.status).json({ ok: false, error: `file fetch ${current.status}` });
    const text = await current.text();

    // wrap for /items/commit-bulk
    const commitBody = {
      message,
      overwrite: true,
      files: [
        {
          path: path.startsWith("items/") ? path : `items/${path}`,
          content_base64: Buffer.from(text).toString("base64"),
          content_type: "application/json"
        }
      ]
    };

    const commit = await fetchWithRetry(
      `${req.protocol}://${req.get("host")}/items/commit-bulk`,
      {
        method: "POST",
        headers: { "Content-Type": "application/json" },
        body: JSON.stringify(commitBody)
      }
    );

    const result = await commit.json();
    res.status(commit.status).json(result);
  } catch (e) {
    res.status(500).json({ ok: false, error: e.message });
  }
});

// --- Startup ---
const PORT = process.env.PORT || 3000;
app.listen(PORT, () => {
  console.log(`[startup] CRT proxy running on ${PORT}`);
  console.log(`[startup] Upstream: ${UPSTREAM_BASE}`);
  console.log(`[startup] Branch:  ${GITHUB_BRANCH}`);
  console.log(`[startup] Repo:    ${GITHUB_REPO}`);
});
