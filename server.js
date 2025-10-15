// server.js — Clean CRT Items/Docs Proxy
// Version 2025-10-13.clean-1a

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

const GITHUB_REPO = process.env.GITHUB_REPO;
const GITHUB_BRANCH = process.env.GITHUB_BRANCH || "main";
const GITHUB_TOKEN = process.env.GITHUB_TOKEN || "";

const CACHE_TTL = parseInt(process.env.CACHE_TTL || "300", 10);
const memoryCache = new Map();

// --- Middleware ---
app.use(morgan("tiny"));
app.use(cors({ origin: "*" }));
app.use(express.json({ limit: "2mb" }));

// --- Helpers ---
const allowedExt = /\.(json|txt|html|xml|csv)$/i;
const safePath = rel => /^[a-z0-9][\w\-./]+$/i.test(rel) && !rel.includes("..") && allowedExt.test(rel);

async function fetchWithRetry(url, { method = "GET", headers = {} } = {}, attempts = 2) {
  let err;
  for (let i = 0; i < attempts; i++) {
    try {
      const r = await fetch(url, { method, headers });
      if (r.status >= 500 && i + 1 < attempts) continue;
      return r;
    } catch (e) { err = e; }
  }
  throw err;
}

// --- /health ---
app.get("/health", (req, res) => res.status(200).send("OK"));

// --- /items/* — Proxy GET from GitHub main/items ---
app.get("/items/*", async (req, res) => {
  const rel = req.params[0];
  if (!safePath(rel)) return res.status(400).json({ error: "Invalid path" });
  const key = `items/${rel}`;
  if (memoryCache.has(key)) {
    const c = memoryCache.get(key);
    if (Date.now() - c.time < CACHE_TTL * 1000) {
      res.type(c.type).send(c.body);
      return;
    }
  }

  const url = `${UPSTREAM_BASE}/items/${rel}`;
  const r = await fetchWithRetry(url);
  if (!r.ok) return res.status(r.status).json({ error: `Upstream ${r.status}` });
  const text = await r.text();
  const type = r.headers.get("content-type") || "application/json";
  memoryCache.set(key, { body: text, type, time: Date.now() });
  res.type(type).send(text);
});

// --- /items/commit — Airtable or item-level commits ---
app.post("/items/commit", async (req, res) => {
  try {
    const { path, json, message } = req.body || {};
    if (!path || json === undefined) return res.status(400).json({ error: "path and json required" });
    const rel = path.replace(/^\/+/, "").replace(/^items\//, "");
    if (!safePath(rel)) return res.status(400).json({ error: "Unsafe path" });

    if (!GITHUB_REPO || !GITHUB_TOKEN) return res.status(500).json({ error: "Missing GitHub credentials" });
    const repoPath = `items/${rel}`;
    const api = `https://api.github.com/repos/${GITHUB_REPO}/contents/${repoPath}`;
    const headers = {
      Authorization: `Bearer ${GITHUB_TOKEN}`,
      Accept: "application/vnd.github+json",
      "User-Agent": "crt-items-proxy"
    };

    let contentStr;
    if (typeof json === "string") contentStr = json;
    else contentStr = JSON.stringify(json, null, 2) + "\n";

    const head = await fetchWithRetry(`${api}?ref=${GITHUB_BRANCH}`, { headers });
    let sha;
    if (head.ok) sha = (await head.json()).sha;

    const body = {
      message: message || `update ${repoPath}`,
      content: Buffer.from(contentStr).toString("base64"),
      branch: GITHUB_BRANCH,
      ...(sha && { sha })
    };
    const put = await fetchWithRetry(api, { method: "PUT", headers, body: JSON.stringify(body) });
    const result = await put.json();
    if (!put.ok) return res.status(put.status).json(result);

    memoryCache.delete(repoPath);
    res.json({ ok: true, commit: { sha: result.commit.sha, url: result.commit.html_url } });
  } catch (e) {
    console.error(e);
    res.status(500).json({ error: e.message });
  }
});

// --- /docs/commit — single JSON doc commit ---
app.post("/docs/commit", async (req, res) => {
  try {
    const { path, json, message } = req.body || {};
    if (!path || json === undefined) return res.status(400).json({ error: "path and json required" });
    const rel = path.replace(/^\/+/, "").replace(/^docs\//, "");
    if (!safePath(rel)) return res.status(400).json({ error: "Unsafe path" });

    const repoPath = `docs/${rel}`;
    const api = `https://api.github.com/repos/${GITHUB_REPO}/contents/${repoPath}`;
    const headers = {
      Authorization: `Bearer ${GITHUB_TOKEN}`,
      Accept: "application/vnd.github+json",
      "User-Agent": "crt-docs-proxy"
    };

    const head = await fetchWithRetry(`${api}?ref=${GITHUB_BRANCH}`, { headers });
    let sha;
    if (head.ok) sha = (await head.json()).sha;

    const contentStr = JSON.stringify(typeof json === "string" ? JSON.parse(json) : json, null, 2) + "\n";
    const body = {
      message: message || `update ${repoPath}`,
      content: Buffer.from(contentStr).toString("base64"),
      branch: GITHUB_BRANCH,
      ...(sha && { sha })
    };
    const put = await fetchWithRetry(api, { method: "PUT", headers, body: JSON.stringify(body) });
    const result = await put.json();
    if (!put.ok) return res.status(put.status).json(result);

    memoryCache.delete(repoPath);
    res.json({ ok: true, commit: { sha: result.commit.sha, url: result.commit.html_url } });
  } catch (e) {
    console.error(e);
    res.status(500).json({ error: e.message });
  }
});

// --- /docs/commit-bulk — Structure Runner (7-file blog publish) ---
app.post("/docs/commit-bulk", async (req, res) => {
  try {
    const { message, overwrite, files } = req.body || {};
    if (!Array.isArray(files) || files.length === 0)
      return res.status(400).json({ error: "files[] required" });

    const headers = {
      Authorization: `Bearer ${GITHUB_TOKEN}`,
      Accept: "application/vnd.github+json",
      "User-Agent": "crt-docs-proxy"
    };

    const refUrl = `https://api.github.com/repos/${GITHUB_REPO}/git/refs/heads/${GITHUB_BRANCH}`;
    const ref = await fetchWithRetry(refUrl, { headers });
    const refJson = await ref.json();
    const baseCommit = refJson.object?.sha;

    const commitResp = await fetchWithRetry(
      `https://api.github.com/repos/${GITHUB_REPO}/git/commits/${baseCommit}`, { headers });
    const baseTree = (await commitResp.json()).tree?.sha;

    const blobShas = [];
    for (const f of files) {
      const b = await fetchWithRetry(`https://api.github.com/repos/${GITHUB_REPO}/git/blobs`, {
        method: "POST",
        headers,
        body: JSON.stringify({ content: f.content_base64, encoding: "base64" })
      });
      const j = await b.json();
      blobShas.push({ path: f.path, sha: j.sha });
    }

    const treeResp = await fetchWithRetry(`https://api.github.com/repos/${GITHUB_REPO}/git/trees`, {
      method: "POST",
      headers,
      body: JSON.stringify({ base_tree: baseTree, tree: blobShas.map(x => ({ path: x.path, mode: "100644", type: "blob", sha: x.sha })) })
    });
    const newTree = (await treeResp.json()).sha;

    const commitPost = await fetchWithRetry(`https://api.github.com/repos/${GITHUB_REPO}/git/commits`, {
      method: "POST",
      headers,
      body: JSON.stringify({ message: message || "bulk publish", tree: newTree, parents: [baseCommit] })
    });
    const newCommit = await commitPost.json();

    await fetchWithRetry(refUrl, {
      method: "PATCH",
      headers,
      body: JSON.stringify({ sha: newCommit.sha, force: !!overwrite })
    });

    res.json({
      ok: true,
      commit: { sha: newCommit.sha, url: newCommit.html_url },
      committed_paths: files.map(f => f.path)
    });
  } catch (e) {
    console.error(e);
    res.status(500).json({ error: e.message });
  }
});

// --- /docs/* — read-only proxy for published outputs ---
app.get("/docs/*", async (req, res) => {
  const rel = req.params[0];
  if (!safePath(rel)) return res.status(400).json({ error: "Invalid path" });
  const url = `${UPSTREAM_BASE}/docs/${rel}`;
  const r = await fetchWithRetry(url);
  if (!r.ok) return res.status(r.status).json({ error: `Upstream ${r.status}` });
  const text = await r.text();
  const type = r.headers.get("content-type") || "application/json";
  res.type(type).send(text);
});

// --- Startup ---
const PORT = process.env.PORT || 3000;
app.listen(PORT, () => {
  console.log(`[startup] CRT proxy running on ${PORT}`);
  console.log(`[startup] Upstream: ${UPSTREAM_BASE}`);
});
