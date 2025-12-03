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
// --- /items/agents/list-runner/command --- primary List-Runner write surface ---
// --- /items/agents/list-runner/command --- primary List-Runner write surface (new) ---
app.post("/items/agents/list-runner/command", async (req, res) => {
  try {
    const { device_id, command, actor, now, intent } = req.body || {};
    if (!device_id || (!command && !intent)) {
      return res.status(400).json({ ok: false, error: "device_id and command or intent required" });
    }

    const ts = typeof now === "string" ? now : new Date().toISOString();
    const baseUrl = `${req.protocol}://${req.get("host")}`;
    const ROOT = "agents/list-runner";
    const LIB = `${ROOT}/lib`;
    const LISTS = `${ROOT}/lists`;
    const LOGS = `${ROOT}/logs`;

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

    function ensureListsShape(started, archived) {
      const s = started && typeof started === "object" ? started : {};
      const a = archived && typeof archived === "object" ? archived : {};
      if (!Array.isArray(s.lists)) s.lists = [];
      if (!Array.isArray(a.lists)) a.lists = [];
      return { started: s, archived: a };
    }

    function ensureLogsShape(updates, txns) {
      const u = updates && typeof updates === "object" ? updates : {};
      const t = txns && typeof txns === "object" ? txns : {};
      if (!Array.isArray(u.events)) u.events = [];
      if (!Array.isArray(t.transactions)) t.transactions = [];
      return { updates: u, txns: t };
    }

    function normalizeStatus(word) {
      const w = String(word || "").toLowerCase().replace(/[_-]+/g, " ").trim();
      if (w === "packed") return "packed";
      if (w === "not packed" || w === "unpacked") return "not_packed";
      if (w === "not needed" || w === "noneeded" || w === "no need") return "not_needed";
      if (w === "missing") return "missing";
      if (w === "broken") return "broken";
      if (w === "left over" || w === "leftover" || w === "left over there") return "left_over";
      if (w.startsWith("sent back")) return "sent_back_early";
      if (w === "reset") return "reset";
      return null;
    }

    function selectList(started, target) {
      const lists = started.lists || [];
      if (!lists.length) return null;

      if (target && target.list_id) {
        const hit = lists.find(l => l.list_id === target.list_id);
        if (hit) return hit;
      }

      if (target && target.show_id) {
        const byShow = lists.filter(l => l.show_id === target.show_id && l.status !== "archived");
        if (byShow.length) return byShow[0];
      }

      const open = lists.filter(l => l.status !== "archived");
      if (open.length) return open[0];

      return lists[0];
    }

    function findLine(list, ref) {
      if (!list || !Array.isArray(list.lines)) return null;
      if (!ref) return null;
      const needle = String(ref).toLowerCase();

      const byId = list.lines.find(line => line.line_id === ref);
      if (byId) return byId;

      return (
        list.lines.find(
          line => String(line.label || "").toLowerCase() === needle
        ) || null
      );
    }

    function addLine(list, payload, libs, ts, device_id, actor) {
      if (!Array.isArray(list.lines)) list.lines = [];

      const line_id =
        payload.line_id ||
        `ln_${Date.now().toString(36)}_${Math.random()
          .toString(36)
          .slice(2, 6)}`;
      const label = payload.label || payload.item_label || payload.name;
      if (!label) return { error: "missing_label" };

      let item_id = payload.item_id || null;
      let type = payload.type || "inventory";

      if (!item_id && libs && Array.isArray(libs.items)) {
        const n = label.toLowerCase();
        const hit = libs.items.find(e => {
          if (!e || !e.name) return false;
          if (String(e.name).toLowerCase() === n) return true;
          if (Array.isArray(e.aliases) && e.aliases.some(a => String(a).toLowerCase() === n)) return true;
          if (Array.isArray(e.misspells) && e.misspells.some(a => String(a).toLowerCase() === n)) return true;
          if (Array.isArray(e.nicknames) && e.nicknames.some(a => String(a).toLowerCase() === n)) return true;
          return false;
        });
        if (hit && hit.item_id) {
          item_id = hit.item_id;
          type = "inventory";
        }
      }

      if (!payload.inventory && type !== "inventory") {
        type = payload.type || "misc";
      }

      const qty =
        typeof payload.qty === "number" && payload.qty > 0
          ? payload.qty
          : 1;

      const line = {
        line_id,
        label,
        item_id: item_id || null,
        type,
        qty,
        to_take: "not_packed",
        to_bring_home: "not_packed",
        notes: payload.notes || "",
        created_at: ts,
        created_by: actor || device_id
      };

      list.lines.push(line);
      return { line };
    }

    function setLineStatus(list, line, status, scope, ts, actor, device_id) {
      const normalized = normalizeStatus(status);
      if (!normalized) return { error: "invalid_status" };

      if (scope !== "to_take" && scope !== "to_bring_home") {
        scope = "to_take";
      }

      if (!line.history) line.history = [];
      const prev = line[scope] || "not_packed";

      line[scope] = normalized === "reset" ? "not_packed" : normalized;

      line.history.push({
        ts,
        by: actor || device_id,
        action: "set_status",
        scope,
        from: prev,
        to: line[scope]
      });

      return { prev, next: line[scope], scope };
    }

    function addComment(list, text, ts, actor, device_id) {
      if (!Array.isArray(list.lines)) list.lines = [];
      const line = {
        line_id: `note_${Date.now().toString(36)}_${Math.random()
          .toString(36)
          .slice(2, 6)}`,
        label: text,
        type: "note",
        qty: 0,
        to_take: "not_packed",
        to_bring_home: "not_packed",
        notes: "",
        created_at: ts,
        created_by: actor || device_id
      };
      list.lines.push(line);
      return { line };
    }

    function applyKit(list, kitIdOrName, libs, ts, device_id, actor) {
      if (!libs || !Array.isArray(libs.kits)) {
        return { error: "no_kits_lib" };
      }
      const key = String(kitIdOrName || "").toLowerCase();
      const kit = libs.kits.find(k => {
        if (!k) return false;
        if (k.kit_id && String(k.kit_id).toLowerCase() === key) return true;
        if (k.name && String(k.name).toLowerCase() === key) return true;
        return false;
      });
      if (!kit || !Array.isArray(kit.items) || !kit.items.length) {
        return { error: "kit_not_found" };
      }

      const added = [];
      for (const ki of kit.items) {
        const payload = {
          label: ki.label || ki.name,
          item_id: ki.item_id,
          qty: ki.qty || 1,
          type: "inventory"
        };
        const res = addLine(list, payload, libs, ts, device_id, actor);
        if (res.line) added.push(res.line);
      }
      return { added, kit };
    }

    function summarizeList(list) {
      if (!list || !Array.isArray(list.lines)) {
        return {
          total: 0,
          to_take_remaining: 0,
          to_bring_home_remaining: 0
        };
      }
      let total = 0;
      let to_take_remaining = 0;
      let to_bring_home_remaining = 0;
      for (const line of list.lines) {
        if (line.type === "note") continue;
        total += 1;
        if (line.to_take === "not_packed") to_take_remaining += 1;
        if (line.to_bring_home === "not_packed")
          to_bring_home_remaining += 1;
      }
      return { total, to_take_remaining, to_bring_home_remaining };
    }

    function recordTx(txns, entry) {
      const tx = {
        tx_id: `tx_${Date.now().toString(36)}_${Math.random()
          .toString(36)
          .slice(2, 8)}`,
        ...entry
      };
      txns.transactions.push(tx);
      return tx;
    }

    function appendUpdate(updates, entry) {
      updates.events.push({
        ts: entry.ts,
        device_id: entry.device_id,
        actor: entry.actor || null,
        type: entry.type,
        message: entry.message || "",
        meta: entry.meta || {}
      });
    }

    function parseCommandString(cmd) {
      if (!cmd || typeof cmd !== "string") return null;
      const raw = cmd.trim();
      const lower = raw.toLowerCase();

      if (lower === "summary" || lower === "show summary") {
        return { type: "summary" };
      }

      let m = lower.match(/^add\s+(.+?)\s+to\s+list\s+(\S+)/);
      if (m) {
        return {
          type: "add_line",
          list_id: m[2],
          label: raw.slice(m.index + 4, m.index + 4 + m[1].length).trim()
        };
      }

      m = lower.match(
        /^mark\s+(.+?)\s+(packed|not\s+packed|not\s+needed|missing|broken|left\s*over|leftover|sent\s+back(?:\s+early)?|reset)$/
      );
      if (m) {
        return {
          type: "set_status",
          line_ref: m[1].trim(),
          status: m[2]
        };
      }

      m = lower.match(/^note\s+(.+)/);
      if (m) {
        return {
          type: "add_comment",
          text: raw.slice(m.index + 5).trim()
        };
      }

      return null;
    }

    function parseIntent(body) {
      if (body.intent && body.intent.type) return body.intent;
      const fromCmd = parseCommandString(body.command);
      if (fromCmd) return fromCmd;
      return null;
    }

    // ---------- load current data ----------

    const [
      showsLib,
      horsesLib,
      itemsLib,
      listsLib,
      locationsLib,
      kitsLib,
      startedRaw,
      archivedRaw,
      updatesRaw,
      txnsRaw
    ] = await Promise.all([
      readJson(`${LIB}/shows_lib.json`, []),
      readJson(`${LIB}/horses_lib.json`, []),
      readJson(`${LIB}/items_lib.json`, []),
      readJson(`${LIB}/lists_lib.json`, []),
      readJson(`${LIB}/locations_lib.json`, []),
      readJson(`${LIB}/kits_lib.json`, []),
      readJson(`${LISTS}/started_lists.json`, { lists: [] }),
      readJson(`${LISTS}/archived_lists.json`, { lists: [] }),
      readJson(`${LOGS}/updates.json`, { events: [] }),
      readJson(`${LOGS}/transactions.json`, { transactions: [] })
    ]);

    const libs = {
      shows: Array.isArray(showsLib) ? showsLib : showsLib.shows || [],
      horses: Array.isArray(horsesLib) ? horsesLib : horsesLib.horses || [],
      items: Array.isArray(itemsLib) ? itemsLib : itemsLib.items || [],
      lists: Array.isArray(listsLib) ? listsLib : listsLib.lists || [],
      locations: Array.isArray(locationsLib)
        ? locationsLib
        : locationsLib.locations || [],
      kits: Array.isArray(kitsLib) ? kitsLib : kitsLib.kits || []
    };

    let { started, archived } = ensureListsShape(startedRaw, archivedRaw);
    let { updates, txns } = ensureLogsShape(updatesRaw, txnsRaw);

    const parsed = parseIntent({ intent, command });
    if (!parsed || !parsed.type) {
      return res
        .status(400)
        .json({ ok: false, error: "unsupported_or_unparsed_command" });
    }

    const target = {
      list_id: parsed.list_id,
      show_id: parsed.show_id
    };
    let list = null;
    if (
      [
        "add_line",
        "set_status",
        "add_comment",
        "apply_kit",
        "summary",
        "show_list"
      ].includes(parsed.type)
    ) {
      list = selectList(started, target);
      if (!list) {
        return res.status(400).json({ ok: false, error: "no_target_list" });
      }
    }

    let action = parsed.type;
    let speech = "";
    let affectedLine = null;

    // ---------- handle intents ----------

    if (parsed.type === "add_line") {
      const payload = {
        line_id: parsed.line_id,
        label: parsed.label || parsed.item_label,
        qty: parsed.qty,
        type: parsed.line_type || parsed.type_hint,
        item_id: parsed.item_id,
        notes: parsed.notes
      };
      const result = addLine(list, payload, libs, ts, device_id, actor);
      if (result.error) {
        return res.status(400).json({ ok: false, error: result.error });
      }
      affectedLine = result.line;
      recordTx(txns, {
        ts,
        device_id,
        actor,
        type: "line_add",
        list_id: list.list_id,
        line_id: affectedLine.line_id,
        meta: { label: affectedLine.label, qty: affectedLine.qty }
      });
      speech = `Added ${affectedLine.label}.`;
    }

    if (parsed.type === "set_status") {
      const line = findLine(
        list,
        parsed.line_ref || parsed.line_id || parsed.item_label
      );
      if (!line) {
        return res
          .status(404)
          .json({ ok: false, error: "line_not_found" });
      }
      const r = setLineStatus(
        list,
        line,
        parsed.status,
        parsed.scope,
        ts,
        actor,
        device_id
      );
      if (r.error) {
        return res.status(400).json({ ok: false, error: r.error });
      }
      affectedLine = line;
      recordTx(txns, {
        ts,
        device_id,
        actor,
        type: "line_status",
        list_id: list.list_id,
        line_id: line.line_id,
        from: r.prev,
        to: r.next,
        meta: { scope: r.scope, label: line.label }
      });
      speech = `${line.label} marked ${r.next}.`;
    }

    if (parsed.type === "add_comment") {
      const text = parsed.text || parsed.comment;
      if (!text) {
        return res
          .status(400)
          .json({ ok: false, error: "missing_comment_text" });
      }
      const r = addComment(list, text, ts, actor, device_id);
      affectedLine = r.line;
      recordTx(txns, {
        ts,
        device_id,
        actor,
        type: "comment_add",
        list_id: list.list_id,
        line_id: r.line.line_id,
        meta: { text }
      });
      speech = "Comment added.";
    }

    if (parsed.type === "apply_kit") {
      const kitKey = parsed.kit_id || parsed.kit_name;
      const r = applyKit(list, kitKey, libs, ts, device_id, actor);
      if (r.error) {
        return res.status(400).json({ ok: false, error: r.error });
      }
      recordTx(txns, {
        ts,
        device_id,
        actor,
        type: "kit_apply",
        list_id: list.list_id,
        meta: {
          kit_id: r.kit.kit_id || null,
          kit_name: r.kit.name || null,
          added_count: r.added.length
        }
      });
      speech = `Applied kit ${r.kit.name || r.kit.kit_id}, ${
        r.added.length
      } lines added.`;
    }

    if (parsed.type === "summary") {
      const counts = summarizeList(list);
      return res.json({
        ok: true,
        action: "summary",
        list_id: list.list_id,
        list_name: list.name,
        counts,
        speech: `${counts.to_take_remaining} items left to pack.`
      });
    }

    if (parsed.type === "show_list") {
      const counts = summarizeList(list);
      return res.json({
        ok: true,
        action: "show_list",
        list_id: list.list_id,
        list_name: list.name,
        counts,
        lines: list.lines || []
      });
    }

    // ---------- log + commit ----------

    appendUpdate(updates, {
      ts,
      device_id,
      actor,
      type: action,
      message: command || action,
      meta: {
        list_id: list && list.list_id,
        line_id: affectedLine && affectedLine.line_id
      }
    });

    const counts = summarizeList(list);

    const files = [
      { path: `${LISTS}/started_lists.json`, json: started },
      { path: `${LISTS}/archived_lists.json`, json: archived },
      { path: `${LOGS}/updates.json`, json: updates },
      { path: `${LOGS}/transactions.json`, json: txns }
    ];

    await commitFiles("list-runner command", files);

    try {
      await runExpeditor();
    } catch (e) {
      return res.status(500).json({
        ok: false,
        error: `expeditor_failed: ${e.message}`
      });
    }

    return res.json({
      ok: true,
      action,
      list_id: list.list_id,
      list_name: list.name,
      counts,
      speech,
      line: affectedLine || undefined
    });
  } catch (e) {
    console.error("list-runner/command error:", e);
    return res.status(500).json({ ok: false, error: e.message });
  }
});

// --- /items/agents/list-runner/state --- read-only snapshot for UI/voice ---
app.get("/items/agents/list-runner/state", async (req, res) => {
  try {
    const baseUrl = `${req.protocol}://${req.get("host")}`;
    const ROOT = "agents/list-runner";
    const LIB = `${ROOT}/lib`;
    const LISTS = `${ROOT}/lists`;

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

    const [
      showsLib,
      horsesLib,
      itemsLib,
      listsLib,
      locationsLib,
      kitsLib,
      startedRaw
    ] = await Promise.all([
      readJson(`${LIB}/shows_lib.json`, []),
      readJson(`${LIB}/horses_lib.json`, []),
      readJson(`${LIB}/items_lib.json`, []),
      readJson(`${LIB}/lists_lib.json`, []),
      readJson(`${LIB}/locations_lib.json`, []),
      readJson(`${LIB}/kits_lib.json`, []),
      readJson(`${LISTS}/started_lists.json`, { lists: [] })
    ]);

    const libs = {
      shows: Array.isArray(showsLib) ? showsLib : showsLib.shows || [],
      horses: Array.isArray(horsesLib) ? horsesLib : horsesLib.horses || [],
      items: Array.isArray(itemsLib) ? itemsLib : itemsLib.items || [],
      lists: Array.isArray(listsLib) ? listsLib : listsLib.lists || [],
      locations: Array.isArray(locationsLib)
        ? locationsLib
        : locationsLib.locations || [],
      kits: Array.isArray(kitsLib) ? kitsLib : kitsLib.kits || []
    };

    const started =
      startedRaw && typeof startedRaw === "object"
        ? startedRaw
        : { lists: [] };
    if (!Array.isArray(started.lists)) started.lists = [];

    return res.json({
      ok: true,
      libs,
      lists: started.lists
    });
  } catch (e) {
    console.error("list-runner/state error:", e);
    return res.status(500).json({ ok: false, error: e.message });
  }
});


// --- /http-get — simple external fetch for runners ---
app.get("/http-get", async (req, res) => {
  try {
    const url = String(req.query.url || "").trim();

    if (!url) {
      return res.status(400).json({ error: "url query param required" });
    }

    // Only allow http/https
    if (!/^https?:\/\//i.test(url)) {
      return res.status(400).json({ error: "url must start with http:// or https://" });
    }

    // Fetch upstream (may be 200, 403, 404, etc.)
    const r = await fetchWithRetry(
      url,
      { method: "GET" },
      { attempts: 2, timeoutMs: 10000 }
    );

    const text = await r.text();

    // Soft 5 MB guard (approx by characters)
    const MAX_CHARS = 5 * 1024 * 1024;
    if (text.length > MAX_CHARS) {
      return res.status(413).json({ error: "upstream_body_too_large" });
    }

    // IMPORTANT: always return 200 so the connector does NOT surface a 4xx error
    // The runner still receives the raw body and can decide how to handle it.
    res
      .status(200)
      .type("text/plain; charset=utf-8")
      .send(text);
  } catch (e) {
    console.error("http-get error:", e);
    res.status(500).json({ error: "http_get_failed", details: e.message });
  }
});

// --- /crt/run — generic runner entry (TOP / BOTTOM / BLOG stub) ---
app.post("/crt/run", async (req, res) => {
  try {
    const body = typeof req.body === "string" ? JSON.parse(req.body) : (req.body || {});
    const { runner, creation_id, mode } = body;

    if (!runner || !creation_id) {
      return res.status(400).json({
        ok: false,
        error: "runner and creation_id are required"
      });
    }

    // Only allow known runners for now
    const RUNNER_SEGMENTS = {
      "crt-top-runner": "top",
      "crt-bottom-runner": "bottom",
      "crt-blog-runner": "blog"
    };

    const segment = RUNNER_SEGMENTS[runner];
    if (!segment) {
      return res.status(400).json({
        ok: false,
        error: `unsupported_runner: ${runner}`
      });
    }

    const where = "crt-items-proxy";
    const modeSafe = mode || "main";
    const ts = new Date().toISOString();

    const pingJson = {
      runner,
      creation_id,
      mode: modeSafe,
      where,
      ts
    };

    const logPath = `docs/runner/${segment}/logs/${creation_id}-ping.json`;

    const baseUrl = `${req.protocol}://${req.get("host")}`;
    const commitUrl = `${baseUrl}/docs/commit`;

    const r = await fetchWithRetry(
      commitUrl,
      {
        method: "POST",
        headers: { "Content-Type": "application/json; charset=utf-8" },
        body: JSON.stringify({
          path: logPath,
          json: pingJson,
          message: `crt-run ping ${runner} ${creation_id}`
        })
      },
      { attempts: 2, timeoutMs: 10000 }
    );

    const out = await r.json().catch(() => ({}));
    if (!r.ok || out.error) {
      return res.status(r.status || 500).json({
        ok: false,
        error: out.error || "docs_commit_failed",
        details: out.details || null
      });
    }

    return res.json({
      ok: true,
      where,
      runner,
      creation_id,
      mode: modeSafe,
      log_path: logPath,
      commit: out.commit || null
    });
  } catch (e) {
    console.error("/crt/run error:", e);
    return res.status(500).json({
      ok: false,
      error: "crt_run_failed",
      details: e.message
    });
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
