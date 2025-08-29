const express = require("express");
const fetch = require("node-fetch");
const cors = require("cors");
const morgan = require("morgan");

require("dotenv").config();

const app = express();

// --- Config via env ---
const UPSTREAM_BASE = process.env.UPSTREAM_BASE; // e.g. https://raw.githubusercontent.com/sportdogfood/clear-round-datasets/gh-pages/items
if (!UPSTREAM_BASE) throw new Error("Missing UPSTREAM_BASE");

const ALLOW_DIRS = new Set(
  (process.env.ALLOW_DIRS || "")
    .split(",")
    .map(s => s.trim())
    .filter(Boolean)
);
const CACHE_TTL = parseInt(process.env.CACHE_TTL || "300", 10);

// --- Tiny in-memory cache ---
const memoryCache = new Map(); // key => { body, etag, lastModified, fetchedAt }

// helpers
function isAllowedPath(rel) {
  if (!rel) return false;
  if (rel.includes("..")) return false;
  if (!/^[a-z0-9\-_/]+\.json$/i.test(rel)) return false;
  const top = rel.split("/")[0];
  return ALLOW_DIRS.has(top);
}
function upstreamUrl(rel) {
  return `${UPSTREAM_BASE}/${rel}`
    .replace(/(?<!:)\/{2,}/g, "/")
    .replace("https:/", "https://");
}

// middleware
app.use(morgan("combined"));
app.use(cors({ origin: "*"}));
app.use(express.json());

// health
app.get("/health", (_req, res) => {
  res.json({ ok: true, uptime: process.uptime() });
});

// proxy: /items/<dir>/<file>.json
app.get("/items/*", async (req, res) => {
  try {
    const rel = (req.params[0] || "").trim();
    if (!isAllowedPath(rel)) return res.status(400).json({ error: "Bad path" });

    const cacheKey = rel;
    const now = Date.now();
    const cached = memoryCache.get(cacheKey);
    const isFresh = cached && (now - cached.fetchedAt) < CACHE_TTL * 1000;

    const headers = {};
    if (cached?.etag) headers["If-None-Match"] = cached.etag;
    if (cached?.lastModified) headers["If-Modified-Since"] = cached.lastModified;

    if (isFresh) {
      res.set("Content-Type", "application/json");
      if (cached.etag) res.set("ETag", cached.etag);
      if (cached.lastModified) res.set("Last-Modified", cached.lastModified);
      return res.status(200).send(cached.body);
    }

    const url = upstreamUrl(rel);
    const resp = await fetch(url, { headers });

    if (resp.status === 304 && cached) {
      cached.fetchedAt = now;
      res.set("Content-Type", "application/json");
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

    memoryCache.set(cacheKey, {
      body: text,
      etag,
      lastModified,
      fetchedAt: now
    });

    res.set("Content-Type", "application/json");
    if (etag) res.set("ETag", etag);
    if (lastModified) res.set("Last-Modified", lastModified);
    return res.status(200).send(text);
  } catch (err) {
    console.error("Proxy error:", err);
    res.status(500).json({ error: "Proxy failed" });
  }
});

// boot
const PORT = process.env.PORT || 3000;
app.listen(PORT, () => {
  console.log(`CRT items proxy running on ${PORT}`);
});
