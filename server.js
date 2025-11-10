// server.js — Clean CRT Items/Docs Proxy (patched)
// Version 2025-10-14.clean-1c

import express from "express";
import fetch from "node-fetch";
import cors from "cors";
import morgan from "morgan";
import dotenv from "dotenv";
import { execFile } from "child_process";
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
// --- /items/agents/list-runner/command --- primary List-Runner write surface ---
app.post("/items/agents/list-runner/command", async (req, res) => {
  try {
    const { device_id, command, now, debug } = req.body || {};
    if (!device_id || !command) {
      return res.status(400).json({ ok: false, error: "device_id and command required" });
    }

    const ts = typeof now === "string" ? now : new Date().toISOString();
    const baseUrl = `${req.protocol}://${req.get("host")}`;
    const ROOT = "agents/list-runner";

    // ---------- helpers ----------
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

    async function commitFiles(message, files) {
      if (!files.length) throw new Error("nothing to commit");
      const body = {
        message: message || "list-runner update",
        overwrite: true,
        files: files.map(f => ({
          path: f.path.startsWith("items/") ? f.path : `items/${f.path}`,
          content_base64: Buffer.from(
            typeof f.raw === "string"
              ? f.raw
              : JSON.stringify(f.json, null, 2) + "\n",
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
        throw new Error(out.error || `commit-bulk ${r.status}`);
      }
      return out;
    }

    function runExpeditor() {
      return new Promise((resolve, reject) => {
        execFile(
          "node",
          [`items/${ROOT}/expeditor.js`, "update"],
          { timeout: 15000 },
          (err, stdout, stderr) => {
            if (err) return reject(new Error(stderr || err.message));
            resolve(stdout || "");
          }
        );
      });
    }

    function toMs(iso) {
      if (!iso) return NaN;
      const t = Date.parse(iso);
      return Number.isNaN(t) ? NaN : t;
    }

    // choose active show: inside, pre-pack (1–3d), next upcoming; allow explicit id/name from command
    function resolveActiveShow(schedule, state, cmd, isoNow) {
      const shows = Array.isArray(schedule?.shows) ? schedule.shows : [];
      if (!shows.length) return null;

      const lower = cmd.toLowerCase();
      const nowMs = toMs(isoNow);

      // explicit: "use show 7129" or "#7129"
      let m = lower.match(/show\s+(\d{3,})/);
      if (!m) m = lower.match(/#(\d{3,})/);
      if (m) {
        const id = m[1];
        const byId = shows.find(s =>
          String(s.show_id) === id ||
          (s.show_name && s.show_name.includes(`#${id}`))
        );
        if (byId) return byId;
      }

      // explicit by name fragment (rough)
      const nameHit = shows.find(s => {
        if (!s.show_name) return false;
        const base = s.show_name.split("(")[0].trim().toLowerCase();
        return base && lower.includes(base);
      });
      if (nameHit) return nameHit;

      // inside a show
      for (const s of shows) {
        const start = toMs(s.start_date);
        const end = toMs(s.end_date);
        if (!Number.isNaN(start) && !Number.isNaN(end) && nowMs >= start && nowMs <= end) {
          return s;
        }
      }

      // pre-pack window 1–3 days before
      let pre = null;
      for (const s of shows) {
        const start = toMs(s.start_date);
        if (Number.isNaN(start) || start <= nowMs) continue;
        const diffDays = (start - nowMs) / 86400000;
        if (diffDays >= 1 && diffDays <= 3) {
          if (!pre || start < toMs(pre.start_date)) pre = s;
        }
      }
      if (pre) return pre;

      // next upcoming
      let next = null;
      for (const s of shows) {
        const start = toMs(s.start_date);
        if (Number.isNaN(start) || start < nowMs) continue;
        if (!next || start < toMs(next.start_date)) next = s;
      }
      if (next) return next;

      // fallback: prior active_show_id if still exists
      if (state?.active_show_id) {
        const prev = shows.find(s => String(s.show_id) === String(state.active_show_id));
        if (prev) return prev;
      }

      // final fallback
      return shows[0];
    }

    // canonicalize status words
    function normalizeStatus(word) {
      const w = (word || "").toLowerCase().replace(/[_-]+/g, " ").trim();
      if (w === "packed") return "packed";
      if (w === "not packed" || w === "unpacked") return "not_packed";
      if (w === "not needed" || w === "noneeded" || w === "no need") return "not_needed";
      if (w === "missing") return "missing";
      if (w === "broken") return "broken";
      if (w === "left over" || w === "leftover" || w === "left over there") return "left_over";
      if (w === "sent back early" || w === "sent back" || w === "sent early") return "sent_back_early";
      if (w === "reset") return "reset";
      return null;
    }

    // parse a natural command into action + args
    function parseCommand(cmd) {
      const raw = cmd.trim();
      const lower = raw.toLowerCase();

      // 1) add <item> to <list>
      let m = lower.match(/^add\s+(.+?)\s+to\s+(tack|equipment|feed)\b/);
      if (m) {
        return {
          action: "add_item",
          item_name: raw.slice(m.index + 4, m.index + 4 + m[1].length).trim(),
          list_name: m[2]
        };
      }

      // 2) mark <item> <status> [by X]
      m = lower.match(/^mark\s+(.+?)\s+(packed|not\s+packed|not\s+needed|missing|broken|left\s*over|leftover|sent\s+back(?:\s+early)?|reset)(?:\s+by\s+(.+))?$/);
      if (m) {
        const itemRaw = raw.slice(m.index + 5, m.index + 5 + m[1].length).trim();
        const statusWord = m[2];
        const confirmByRaw = m[3] ? raw.substring(raw.toLowerCase().indexOf(m[3])).replace(/^by\s+/i, "").trim() : null;
        const status = normalizeStatus(statusWord);
        if (!status) return { action: null };
        return {
          action: status === "reset" ? "reset_item" : "set_status",
          item_name: itemRaw,
          status,
          confirm_by: confirmByRaw || null
        };
      }

      // 3) bulk pack to_take
      if (lower.includes("make sure") && lower.includes("packed") && lower.includes("take")) {
        return { action: "bulk_pack_to_take" };
      }

      // 4) bulk pack to_bring_home
      if (
        lower.includes("make sure") &&
        lower.includes("packed") &&
        (lower.includes("leave") || lower.includes("home"))
      ) {
        return { action: "bulk_pack_to_bring_home" };
      }

      return { action: null };
    }

    // ensure show node + default lists under started_lists.json
    function ensureShow(started, show) {
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
      const s = started.shows[key];
      if (!s.lists) s.lists = {};
      for (const name of ["tack", "equipment", "feed"]) {
        if (!s.lists[name]) s.lists[name] = [];
      }
      return { key, showState: s };
    }

    function ensureStateScaffold(state) {
      if (!state.devices) state.devices = {};
      if (!state.items) state.items = {};
    }

    function touchDevice(state, device_id, ip, ts) {
      ensureStateScaffold(state);
      if (!state.devices[device_id]) state.devices[device_id] = {};
      state.devices[device_id].last_seen = ts;
      if (ip) state.devices[device_id].last_ip = ip;
    }

    // item registry lookup: uses lists/item_registry.json (array of objects)
    function buildItemRegistryMap(reg) {
      const map = {};
      if (!Array.isArray(reg)) return map;
      for (const e of reg) {
        if (!e || !e.name) continue;
        const key = e.name.toLowerCase();
        map[key] = e;
        if (Array.isArray(e.aliases)) {
          for (const a of e.aliases) {
            if (a) map[a.toLowerCase()] = e;
          }
        }
        if (Array.isArray(e.mispells)) {
          for (const m of e.mispells) {
            if (m) map[m.toLowerCase()] = e;
          }
        }
      }
      return map;
    }

    function resolveItemDefinition(name, listName, regMap) {
      const n = name.toLowerCase();
      const hit = regMap[n];
      if (hit) {
        return {
          name: hit.name || name,
          type: hit.type || listName || null,
          subtype: hit.subtype || null,
          note: hit.note || null
        };
      }
      // default from list name
      let type = null;
      if (listName === "tack") type = "tack";
      else if (listName === "equipment") type = "equipment";
      else if (listName === "feed") type = "feed";
      return { name, type, subtype: null, note: null };
    }

    function globalItemStatus(state, itemName) {
      ensureStateScaffold(state);
      const k = itemName.toLowerCase();
      return state.items[k] || null;
    }

    function setGlobalItemStatus(state, itemName, status, where, ts) {
      ensureStateScaffold(state);
      const k = itemName.toLowerCase();
      if (!state.items[k]) state.items[k] = { name: itemName };
      if (status === "reset" || status === "packed" || status === "not_packed" || status === "not_needed") {
        // treat as cleared / ok
        state.items[k].status = "ok";
        state.items[k].where = where || null;
      } else {
        state.items[k].status = status;
        state.items[k].where = where || null;
      }
      state.items[k].updated_at = ts;
    }

    function addItemToList(showState, listName, itemName, regMap, state, ts) {
      const lname = listName.toLowerCase();
      const list = showState.lists[lname] || (showState.lists[lname] = []);

      // block if globally missing/broken unless reset
      const global = globalItemStatus(state, itemName);
      if (global && (global.status === "missing" || global.status === "broken")) {
        return { added: false, blocked: true, reason: global.status };
      }

      const exists = list.some(
        it => (it.name || "").toLowerCase() === itemName.toLowerCase()
      );
      if (exists) return { added: false, blocked: false };

      const def = resolveItemDefinition(itemName, lname, regMap);
      const id = `itm_${Date.now().toString(36)}_${Math.random().toString(36).slice(2, 8)}`;
      list.push({
        id,
        name: def.name,
        type: def.type,
        subtype: def.subtype,
        note: def.note,
        created_at: ts,
        to_take: "not_packed",
        to_bring_home: null,
        history: [
          { ts, action: "create", by: device_id }
        ]
      });

      // initialize global status as ok/home
      setGlobalItemStatus(state, def.name, "ok", "home", ts);

      return { added: true, blocked: false };
    }

    function findItemOnShow(showState, itemName) {
      const needle = itemName.toLowerCase();
      let found = null;
      for (const [lname, arr] of Object.entries(showState.lists || {})) {
        for (let i = 0; i < arr.length; i++) {
          const it = arr[i];
          if ((it.name || "").toLowerCase() === needle) {
            if (found) {
              return { error: "ambiguous" };
            }
            found = { listName: lname, index: i, item: it };
          }
        }
      }
      if (!found) return { error: "not_found" };
      return found;
    }

    function applyStatusToItem(showState, state, parsed, ts) {
      const { item_name, status, confirm_by } = parsed;
      const located = findItemOnShow(showState, item_name);
      if (located.error) return { changed: false, error: located.error };

      const { listName, index, item } = located;
      const showMode = showState.state || "home";
      const key = (showMode === "away" || showMode === "complete")
        ? "to_bring_home"
        : "to_take";

      if (status === "reset") {
        const prev = item[key];
        item[key] = "not_packed";
        if (!item.history) item.history = [];
        item.history.push({
          ts,
          action: "reset",
          from: prev || null,
          to: item[key],
          by: confirm_by || device_id
        });
        setGlobalItemStatus(state, item.name, "ok", null, ts);
        return { changed: true, listName, item };
      }

      const canonical = status;
      const prev = item[key] || null;
      if (prev === canonical) {
        return { changed: false, listName, item };
      }

      item[key] = canonical;
      if (!item.history) item.history = [];
      item.history.push({
        ts,
        action: "set_status",
        field: key,
        from: prev,
        to: canonical,
        by: confirm_by || device_id
      });

      // update global status on serious outcomes
      if (["missing", "broken", "left_over", "sent_back_early"].includes(canonical)) {
        const where =
          canonical === "left_over"
            ? `show:${showState.show_id || ""}`
            : null;
        setGlobalItemStatus(state, item.name, canonical, where, ts);
      } else if (canonical === "packed" || canonical === "not_needed") {
        // packed/not_needed tends toward ok; we do not force where
        setGlobalItemStatus(state, item.name, canonical, null, ts);
      }

      return { changed: true, listName, item };
    }

    function bulkPack(showState, state, mode, ts) {
      let count = 0;
      const key = mode === "to_bring_home" ? "to_bring_home" : "to_take";
      for (const arr of Object.values(showState.lists || {})) {
        for (const it of arr) {
          if (it[key] === "not_packed") {
            const prev = it[key];
            it[key] = "packed";
            if (!it.history) it.history = [];
            it.history.push({
              ts,
              action: "bulk_pack",
              field: key,
              from: prev,
              to: "packed",
              by: device_id
            });
            setGlobalItemStatus(state, it.name, "packed", null, ts);
            count++;
          }
        }
      }
      return count;
    }

    function summarize(showState) {
      let total = 0;
      let to_take_unpacked = 0;
      let to_bring_home_unpacked = 0;
      for (const arr of Object.values(showState.lists || {})) {
        for (const it of arr) {
          total++;
          if (it.to_take === "not_packed") to_take_unpacked++;
          if (it.to_bring_home === "not_packed") to_bring_home_unpacked++;
        }
      }
      return { total_items: total, to_take_unpacked, to_bring_home_unpacked };
    }

    function autoArchiveOldShows(started, archived, ts) {
      const nowMs = toMs(ts);
      if (!started.shows) return;
      if (!archived.shows) archived.shows = {};
      for (const [key, s] of Object.entries(started.shows)) {
        const end = toMs(s.end_date);
        if (Number.isNaN(end)) continue;
        const diffDays = (nowMs - end) / 86400000;
        if (diffDays >= 3) {
          archived.shows[key] = s;
          delete started.shows[key];
        }
      }
    }

    // ---------- load current state ----------
    const [stateRaw, schedule, startedRaw, archivedRaw, updatesRaw, itemRegRaw] =
      await Promise.all([
        readJson(`${ROOT}/state.json`, {}),
        readJson(`${ROOT}/shows/show_schedule.json`, { shows: [] }),
        readJson(`${ROOT}/lists/started_lists.json`, { shows: {} }),
        readJson(`${ROOT}/lists/archived_lists.json`, { shows: {} }),
        readJson(`${ROOT}/logs/updates.json`, { events: [] }),
        readJson(`${ROOT}/lists/item_registry.json`, [])
      ]);

    const state = stateRaw || {};
    const started = startedRaw || { shows: {} };
    const archived = archivedRaw || { shows: {} };
    const updates = updatesRaw || { events: [] };
    if (!Array.isArray(updates.events)) updates.events = [];

    touchDevice(state, device_id, req.ip, ts);

    const regMap = buildItemRegistryMap(itemRegRaw);
    const activeShow = resolveActiveShow(schedule, state, command, ts);
    if (!activeShow) {
      return res.status(500).json({ ok: false, error: "no shows in schedule" });
    }

    const parsed = parseCommand(command);
    if (!parsed.action) {
      return res.status(400).json({ ok: false, error: "unsupported command" });
    }

    const { key: showKey, showState } = ensureShow(started, activeShow);

    // ---------- apply command ----------
    let actionLabel = parsed.action;
    let bulkCount = 0;
    let changed = false;
    let errorDetail = null;

    if (parsed.action === "add_item") {
      const out = addItemToList(showState, parsed.list_name, parsed.item_name, regMap, state, ts);
      if (out.blocked) {
        actionLabel = `blocked_${out.reason}`;
        errorDetail = `item locked as ${out.reason}`;
      } else if (out.added) {
        changed = true;
      } else {
        actionLabel = "noop_item_exists";
      }
    } else if (parsed.action === "set_status") {
      const r = applyStatusToItem(showState, state, parsed, ts);
      if (r.error === "not_found") {
        return res.status(400).json({ ok: false, error: "item not found" });
      }
      if (r.error === "ambiguous") {
        return res.status(400).json({ ok: false, error: "item ambiguous" });
      }
      changed = r.changed;
    } else if (parsed.action === "reset_item") {
      const r = applyStatusToItem(showState, state, parsed, ts);
      if (r.error === "not_found") {
        return res.status(400).json({ ok: false, error: "item not found" });
      }
      if (r.error === "ambiguous") {
        return res.status(400).json({ ok: false, error: "item ambiguous" });
      }
      changed = r.changed;
    } else if (parsed.action === "bulk_pack_to_take") {
      bulkCount = bulkPack(showState, state, "to_take", ts);
      changed = bulkCount > 0;
    } else if (parsed.action === "bulk_pack_to_bring_home") {
      bulkCount = bulkPack(showState, state, "to_bring_home", ts);
      changed = bulkCount > 0;
    }

    // update state header
    state.active_show_id = activeShow.show_id;
    state.active_show_name = activeShow.show_name;
    state.updated_at = ts;

    // auto-archive by age (3+ days after end)
    autoArchiveOldShows(started, archived, ts);

    // log event (even if no-op)
    const files_touched = [
      `${ROOT}/state.json`,
      `${ROOT}/lists/started_lists.json`,
      `${ROOT}/logs/updates.json`
    ];
    const event = {
      ts,
      device_id,
      ip: req.ip,
      show_id: activeShow.show_id,
      show_name: activeShow.show_name,
      show_key: showKey,
      command_raw: command,
      action: actionLabel,
      bulk_count: bulkCount,
      error: errorDetail || null,
      files_touched,
      success: errorDetail ? false : true
    };
    updates.events.push(event);

    // prepare commits
    const files = [
      { path: `items/${ROOT}/state.json`, json: state },
      { path: `items/${ROOT}/lists/started_lists.json`, json: started },
      { path: `items/${ROOT}/logs/updates.json`, json: updates }
    ];
    if (archived && Object.keys(archived.shows || {}).length) {
      files.push({
        path: `items/${ROOT}/lists/archived_lists.json`,
        json: archived
      });
    }

    await commitFiles("list-runner command", files);

    // run expeditor to refresh lists/index.json etc.
    try {
      await runExpeditor();
    } catch (e) {
      return res.status(500).json({ ok: false, error: `expeditor failed: ${e.message}` });
    }

    const counts = summarize(showState);
    const resp = {
      ok: !errorDetail,
      show_id: activeShow.show_id,
      show_name: activeShow.show_name,
      action: actionLabel,
      bulk_count: bulkCount || undefined,
      counts
    };
    if (errorDetail) resp.error = errorDetail;
    if (debug) {
      resp.debug = { parsed, event };
    }

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
