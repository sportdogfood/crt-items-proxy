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
// --- /items/agents/list-runner/command --- mobile-friendly List-Runner surface ---
app.post("/items/agents/list-runner/command", async (req, res) => {
  try {
    const { device_id, command, actor, now, debug } = req.body || {};
    if (!device_id || !command) {
      return res.status(400).json({ ok: false, error: "device_id and command required" });
    }

    const baseUrl = `${req.protocol}://${req.get("host")}`;
    const ts = typeof now === "string" ? now : new Date().toISOString();
    const ROOT = "agents/list-runner";

    // --- helpers (scoped to this route) ---

    function uid() {
      return (
        "lr_" +
        Date.now().toString(36) +
        "_" +
        Math.random().toString(36).slice(2, 8)
      );
    }

    async function readJson(rel, fallback) {
      const url = `${baseUrl}/items/${rel}`;
      const r = await fetchWithRetry(url, { method: "GET" });
      if (r.status === 404) return fallback;
      if (!r.ok) throw new Error(`read ${rel} ${r.status}`);
      const text = await r.text();
      if (!text) return fallback;
      try {
        return JSON.parse(text);
      } catch (e) {
        throw new Error(`parse ${rel}: ${e.message}`);
      }
    }

    async function commitBulk(message, files) {
      if (!files || !files.length) {
        throw new Error("nothing to commit");
      }
      const body = {
        message: message || "list-runner update",
        overwrite: true,
        files: files.map(f => ({
          path: f.path,
          content_base64: Buffer.from(
            typeof f.json === "string" ? f.json : JSON.stringify(f.json, null, 2) + "\n",
            "utf8"
          ).toString("base64"),
          content_type: "application/json"
        }))
      };
      const url = `${baseUrl}/items/commit-bulk`;
      const r = await fetchWithRetry(url, {
        method: "POST",
        headers: { "Content-Type": "application/json" },
        body: JSON.stringify(body)
      });
      const out = await r.json().catch(() => ({}));
      if (!r.ok || out.ok === false) {
        throw new Error(out.error || `commit-bulk failed (${r.status})`);
      }
      return out;
    }

    function normalizeListName(name, listRegistry) {
      if (!name) return null;
      const key = name.toLowerCase().trim();
      // direct
      if (listRegistry[key]) return key;
      // aliases/misspells
      for (const [lname, meta] of Object.entries(listRegistry)) {
        const { aliases = [], mispells = [] } = meta || {};
        if (
          aliases.some(a => a.toLowerCase() === key) ||
          mispells.some(a => a.toLowerCase() === key)
        ) {
          return lname;
        }
      }
      return null;
    }

    function normalizeItemName(name, itemRegistry) {
      if (!name) return { canonical: name, blocked: false };
      const key = name.toLowerCase().trim();
      for (const [iname, meta] of Object.entries(itemRegistry || {})) {
        if (iname.toLowerCase() === key) {
          return { canonical: iname, blocked: !!meta.blocked };
        }
        const { aliases = [], mispells = [], nicknames = [] } = meta || {};
        if (
          aliases.concat(mispells, nicknames || []).some(a => a.toLowerCase() === key)
        ) {
          return { canonical: iname, blocked: !!meta.blocked };
        }
      }
      return { canonical: name, blocked: false };
    }

    function pickStatusToken(raw) {
      const v = raw.toLowerCase();
      if (v === "packed") return "packed";
      if (v === "not_packed" || v === "unpacked") return "not_packed";
      if (v === "notneeded" || v === "not_needed") return "not_needed";
      if (v === "missing") return "missing";
      if (v === "broken") return "broken";
      if (v === "left_over" || v === "leftover") return "left_over";
      if (v === "sent_back_early" || v === "sent-back-early") return "sent_back_early";
      return null;
    }

    function parseCommand(input, listRegistry) {
      const text = String(input).trim();
      const lower = text.toLowerCase();

      // add <item> to <list>
      let m = lower.match(/^add\s+(.+?)\s+to\s+([a-z0-9 _-]+)$/);
      if (m) {
        return {
          kind: "add_item",
          raw_item: m[1].trim(),
          raw_list: m[2].trim()
        };
      }

      // mark <item> as <status>
      m = lower.match(/^mark\s+(.+?)\s+as\s+([a-z_ -]+)$/);
      if (m) {
        return {
          kind: "item_status",
          raw_item: m[1].trim(),
          raw_status: m[2].trim()
        };
      }

      // mark <item> <status>
      m = lower.match(/^mark\s+(.+?)\s+([a-z_ -]+)$/);
      if (m) {
        return {
          kind: "item_status",
          raw_item: m[1].trim(),
          raw_status: m[2].trim()
        };
      }

      // mark <list> away
      m = lower.match(/^mark\s+([a-z0-9 _-]+)\s+away$/);
      if (m) {
        return {
          kind: "list_state",
          raw_list: m[1].trim(),
          list_state: "away"
        };
      }

      // mark <list> complete
      m = lower.match(/^mark\s+([a-z0-9 _-]+)\s+complete$/);
      if (m) {
        return {
          kind: "list_state",
          raw_list: m[1].trim(),
          list_state: "complete"
        };
      }

      // show <list>
      m = lower.match(/^show\s+(?:me\s+)?(?:my\s+)?([a-z0-9 _-]+)\s+list$/);
      if (m) {
        return {
          kind: "show_list",
          raw_list: m[1].trim()
        };
      }

      // summary
      if (lower === "summary" || lower === "show summary") {
        return { kind: "summary" };
      }

      return { kind: "unsupported" };
    }

    function selectActiveShow(showsData, isoNow) {
      if (!showsData || !Array.isArray(showsData.shows)) return null;
      const nowDate = new Date(isoNow);
      let candidate = null;

      for (const show of showsData.shows) {
        if (!show.start_date || !show.end_date) continue;
        const s = new Date(show.start_date);
        const e = new Date(show.end_date);

        // active window: start - 1 day to end + 3 days
        const pre = new Date(s);
        pre.setUTCDate(pre.getUTCDate() - 1);
        const post = new Date(e);
        post.setUTCDate(post.getUTCDate() + 3);

        if (nowDate >= pre && nowDate <= post) {
          if (!candidate || new Date(candidate.start_date) < s) {
            candidate = show;
          }
        }
      }

      if (candidate) return candidate;

      // otherwise next upcoming
      let nextShow = null;
      for (const show of showsData.shows) {
        if (!show.start_date) continue;
        const s = new Date(show.start_date);
        if (s > nowDate) {
          if (!nextShow || s < new Date(nextShow.start_date)) {
            nextShow = show;
          }
        }
      }
      return nextShow;
    }

    function ensureShowContainer(started, show) {
      if (!started.version) started.version = "1.0";
      if (!started.shows) started.shows = {};
      const key = String(show.show_id);
      if (!started.shows[key]) {
        started.shows[key] = {
          show_id: show.show_id,
          show_name: show.show_name,
          start_date: show.start_date,
          end_date: show.end_date,
          state: "home",
          lists: {}
        };
      }
      return started.shows[key];
    }

    function ensureList(showEntry, listName) {
      if (!showEntry.lists[listName]) {
        showEntry.lists[listName] = {
          name: listName,
          state: showEntry.state === "away" ? "away" : "home",
          items: []
        };
      }
      return showEntry.lists[listName];
    }

    function findItem(list, name) {
      const key = name.toLowerCase();
      const items = list.items || [];
      for (let i = 0; i < items.length; i++) {
        if ((items[i].name || "").toLowerCase() === key) {
          return { index: i, item: items[i] };
        }
      }
      return null;
    }

    function applyItemStatus(list, item, statusToken) {
      const before = {
        to_take: item.to_take || "not_packed",
        to_bring_home: item.to_bring_home || "not_packed"
      };

      const isHome = list.state === "home";
      const isAway = list.state === "away";

      // valid transitions
      const validHome = ["not_packed", "packed", "not_needed", "missing", "broken"];
      const validAway = [
        "not_packed",
        "packed",
        "missing",
        "broken",
        "left_over",
        "sent_back_early"
      ];

      if (isHome && !validHome.includes(statusToken)) {
        return { changed: false, reason: "invalid_status_for_home" };
      }
      if (isAway && !validAway.includes(statusToken)) {
        return { changed: false, reason: "invalid_status_for_away" };
      }

      if (isHome) {
        item.to_take = statusToken;
      } else if (isAway) {
        item.to_bring_home = statusToken;
      } else {
        // default home-like
        item.to_take = statusToken;
      }

      // block on missing/broken
      if (statusToken === "missing" || statusToken === "broken") {
        item.blocked = true;
      }

      const after = {
        to_take: item.to_take,
        to_bring_home: item.to_bring_home
      };
      const changed =
        before.to_take !== after.to_take ||
        before.to_bring_home !== after.to_bring_home;

      return { changed, before, after };
    }

    function buildIndex(started) {
      const idx = {
        version: "1.0",
        updated_at: ts,
        shows: {}
      };
      if (!started.shows) return idx;

      for (const [sid, s] of Object.entries(started.shows)) {
        const showEntry = {
          state: s.state || "home",
          lists: {}
        };
        for (const [lname, l] of Object.entries(s.lists || {})) {
          const items = l.items || [];
          let total = items.length;
          let to_take_remaining = 0;
          let to_bring_home_remaining = 0;

          for (const it of items) {
            if ((l.state || "home") === "home") {
              if (!it.to_take || it.to_take === "not_packed") {
                to_take_remaining++;
              }
            } else if ((l.state || "home") === "away") {
              if (!it.to_bring_home || it.to_bring_home === "not_packed") {
                to_bring_home_remaining++;
              }
            }
          }

          showEntry.lists[lname] = {
            state: l.state || "home",
            total_items: total,
            to_take_remaining,
            to_bring_home_remaining
          };
        }
        idx.shows[sid] = showEntry;
      }

      return idx;
    }

    function computeHints(list) {
      const items = list.items || [];
      let to_take_done = 0;
      let to_take_total = 0;
      let to_bring_done = 0;
      let to_bring_total = 0;

      for (const it of items) {
        if (list.state === "home") {
          to_take_total++;
          if (
            it.to_take === "packed" ||
            it.to_take === "not_needed"
          ) {
            to_take_done++;
          }
        } else if (list.state === "away") {
          to_bring_total++;
          if (
            it.to_bring_home === "packed" ||
            it.to_bring_home === "missing" ||
            it.to_bring_home === "broken" ||
            it.to_bring_home === "left_over" ||
            it.to_bring_home === "sent_back_early"
          ) {
            to_bring_done++;
          }
        }
      }

      const hints = {};
      if (list.state === "home" && to_take_total > 0 && to_take_done === to_take_total) {
        hints.ready_to_mark_away = true;
      }
      if (list.state === "away" && to_bring_total > 0 && to_bring_done === to_bring_total) {
        hints.ready_to_mark_complete = true;
      }
      return {
        to_take_total,
        to_take_done,
        to_bring_total,
        to_bring_done,
        hints
      };
    }

    // --- load current data ---

    const [
      showsData,
      stateRaw,
      startedRaw,
      archivedRaw,
      itemRegistryRaw,
      listRegistryRaw,
      updatesRaw
    ] = await Promise.all([
      readJson(`${ROOT}/shows/show_schedule.json`, { shows: [] }),
      readJson(`${ROOT}/state.json`, {}),
      readJson(`${ROOT}/lists/started_lists.json`, { version: "1.0", shows: {} }),
      readJson(`${ROOT}/lists/archived_lists.json`, { version: "1.0", shows: {} }),
      readJson(`${ROOT}/lists/item_registry.json`, { item_registry: {} }),
      readJson(`${ROOT}/lists/list_registry.json`, { list_registry: {} }),
      readJson(`${ROOT}/logs/updates.json`, { events: [] })
    ]);

    const state = stateRaw || {};
    const started = startedRaw || { version: "1.0", shows: {} };
    const archived = archivedRaw || { version: "1.0", shows: {} };
    const itemRegistry = (itemRegistryRaw && itemRegistryRaw.item_registry) || {};
    const listRegistry = (listRegistryRaw && listRegistryRaw.list_registry) || {};
    const updates = updatesRaw || { events: [] };
    if (!Array.isArray(updates.events)) updates.events = [];

    const cmd = parseCommand(command, listRegistry);

    if (cmd.kind === "unsupported") {
      return res.status(400).json({ ok: false, error: "unsupported_command" });
    }

    const activeShow = selectActiveShow(showsData, ts);
    if (!activeShow) {
      return res.status(400).json({ ok: false, error: "no_active_or_upcoming_show" });
    }

    const showEntry = ensureShowContainer(started, activeShow);
    state.last_command_at = ts;
    state.last_device_id = device_id;
    if (actor) state.last_actor = actor;
    state.active_show_id = activeShow.show_id;

    let targetList = null;
    let targetItem = null;
    let statusToken = null;
    let eventNote = "";
    let listSummary = null;

    // --- execute command ---

    if (cmd.kind === "add_item") {
      const listKey = normalizeListName(cmd.raw_list, listRegistry) || cmd.raw_list.toLowerCase();
      const list = ensureList(showEntry, listKey);

      const { canonical, blocked } = normalizeItemName(cmd.raw_item, itemRegistry);
      if (blocked) {
        return res.status(400).json({ ok: false, error: "item_blocked_missing_or_broken" });
      }

      const existing = findItem(list, canonical);
      if (!existing) {
        const it = {
          uid: uid(),
          name: canonical,
          created_at: ts,
          to_take: "not_packed",
          to_bring_home: "not_packed",
          blocked: false
        };
        list.items.push(it);
        targetList = list;
        targetItem = it;
        eventNote = "add_item";
      } else {
        targetList = list;
        targetItem = existing.item;
        eventNote = "add_item_duplicate_ignored";
      }

      listSummary = computeHints(list);
    }

    if (cmd.kind === "item_status") {
      statusToken = pickStatusToken(cmd.raw_status || "");
      if (!statusToken) {
        return res.status(400).json({ ok: false, error: "invalid_status" });
      }

      // search across all lists in active show
      let foundRef = null;
      for (const [lname, l] of Object.entries(showEntry.lists || {})) {
        const m = findItem(l, cmd.raw_item);
        if (m) {
          foundRef = { listName: lname, list: l, index: m.index, item: m.item };
          break;
        }
      }

      if (!foundRef) {
        return res.status(400).json({ ok: false, error: "item_not_found_in_active_show" });
      }

      const { listName, list, item } = foundRef;
      const apply = applyItemStatus(list, item, statusToken);
      if (!apply.changed) {
        eventNote = apply.reason || "no_change";
      } else {
        eventNote = "item_status_update";
      }

      targetList = list;
      targetItem = item;
      listSummary = computeHints(list);
    }

    if (cmd.kind === "list_state") {
      const listKey = normalizeListName(cmd.raw_list, listRegistry) || cmd.raw_list.toLowerCase();
      const list = ensureList(showEntry, listKey);

      if (cmd.list_state === "away") {
        // caller confirms they are shipping out
        list.state = "away";
        eventNote = "list_mark_away_confirmed";
      } else if (cmd.list_state === "complete") {
        list.state = "complete";
        eventNote = "list_mark_complete_confirmed";
      }

      targetList = list;
      listSummary = computeHints(list);
    }

    if (cmd.kind === "show_list") {
      const listKey = normalizeListName(cmd.raw_list, listRegistry) || cmd.raw_list.toLowerCase();
      const list = ensureList(showEntry, listKey);
      targetList = list;
      listSummary = computeHints(list);

      const compact = (list.items || []).map(it => ({
        name: it.name,
        to_take: it.to_take || "not_packed",
        to_bring_home: it.to_bring_home || "not_packed"
      }));

      const resp = {
        ok: true,
        show_id: activeShow.show_id,
        show_name: activeShow.show_name,
        list: list.name,
        list_state: list.state,
        counts: {
          total_items: list.items.length,
          to_take_total: listSummary.to_take_total,
          to_take_done: listSummary.to_take_done,
          to_bring_home_total: listSummary.to_bring_total,
          to_bring_home_done: listSummary.to_bring_done
        },
        hints: listSummary.hints,
        items: compact
      };
      return res.json(resp);
    }

    if (cmd.kind === "summary") {
      const index = buildIndex(started);
      return res.json({
        ok: true,
        show_id: activeShow.show_id,
        show_name: activeShow.show_name,
        index
      });
    }

    // if we reach here, we mutated or attempted to mutate

    const index = buildIndex(started);

    // --- log event ---

    const evt = {
      id: uid(),
      ts,
      device_id,
      actor: actor || null,
      command_raw: command,
      show_id: activeShow.show_id,
      show_name: activeShow.show_name,
      list_name: targetList ? targetList.name : null,
      item_uid: targetItem ? targetItem.uid : null,
      item_name: targetItem ? targetItem.name : null,
      status_to_take: targetItem ? targetItem.to_take || null : null,
      status_to_bring_home: targetItem ? targetItem.to_bring_home || null : null,
      list_state: targetList ? targetList.state || null : null,
      note: eventNote
    };
    updates.events.push(evt);

    // --- prepare commits ---

    const files = [
      {
        path: `items/${ROOT}/state.json`,
        json: state
      },
      {
        path: `items/${ROOT}/lists/started_lists.json`,
        json: started
      },
      {
        path: `items/${ROOT}/lists/index.json`,
        json: index
      },
      {
        path: `items/${ROOT}/logs/updates.json`,
        json: updates
      }
    ];

    // archived.json is untouched in this minimal route; include only if you later move shows.

    await commitBulk("list-runner command", files);

    // --- response summary for mobile / voice ---

    const summaryCounts =
      targetList && listSummary
        ? {
            total_items: targetList.items.length,
            to_take_total: listSummary.to_take_total,
            to_take_done: listSummary.to_take_done,
            to_bring_home_total: listSummary.to_bring_total,
            to_bring_home_done: listSummary.to_bring_done
          }
        : null;

    const speechParts = [];
    if (targetList && summaryCounts) {
      if (targetList.state === "home") {
        speechParts.push(
          `${summaryCounts.to_take_done} of ${summaryCounts.to_take_total} to-take items are set.`
        );
        if (listSummary.hints.ready_to_mark_away) {
          speechParts.push("This list is ready to mark away.");
        }
      } else if (targetList.state === "away") {
        speechParts.push(
          `${summaryCounts.to_bring_home_done} of ${summaryCounts.to_bring_home_total} return items are set.`
        );
        if (listSummary.hints.ready_to_mark_complete) {
          speechParts.push("This list is ready to mark complete.");
        }
      }
    }

    const resp = {
      ok: true,
      show_id: activeShow.show_id,
      show_name: activeShow.show_name,
      list: targetList ? targetList.name : null,
      list_state: targetList ? targetList.state : null,
      item: targetItem
        ? {
            uid: targetItem.uid,
            name: targetItem.name,
            to_take: targetItem.to_take,
            to_bring_home: targetItem.to_bring_home,
            blocked: !!targetItem.blocked
          }
        : null,
      counts: summaryCounts,
      hints: listSummary ? listSummary.hints : {},
      speech: speechParts.join(" "),
      debug: debug
        ? {
            parsed: cmd,
            event: evt
          }
        : undefined
    };

    return res.json(resp);
  } catch (e) {
    console.error("list-runner/command error:", e);
    return res.status(500).json({ ok: false, error: e.message });
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
