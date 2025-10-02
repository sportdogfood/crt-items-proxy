// ---------- Shared handler for GET/HEAD on /items/* ----------
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

// --- STASH endpoint ---
app.post("/stash", express.json({ limit: "5mb" }), async (req, res) => {
  try {
    const { folder, filename_base, files } = req.body || {};
    if (!Array.isArray(files) || files.length === 0) return res.status(400).json({ error: "files[] required" });

    const f = safeSeg(folder) || "misc";
    const b = safeSeg(filename_base) || new Date().toISOString().replace(/[:.]/g, "").slice(0, 15);

    const out = [];
    for (const file of files) {
      if (!file || !file.name || file.body === undefined) return res.status(400).json({ error: "each file requires name and body" });
      const rel = joinStashPath(f, b, file.name);
      if (!rel) return res.status(400).json({ error: `bad file name: ${file.name}` });
      if (!ALLOWED_FILE_EXT_RE.test(rel)) return res.status(400).json({ error: `extension not allowed for ${file.name}` });

      const result = await commitText(rel, String(file.body), `stash(${f}/${b}): ${file.name}`);
      const publicUrl = `${req.protocol}://${req.get("host")}/items/${rel.replace(/^items\//, "")}`;
      out.push({ name: file.name, path: rel, url: publicUrl, commit: result.commit });
    }

    return res.json({ ok: true, folder: f, filename_base: b, files: out });
  } catch (err) {
    console.error("stash error:", err);
    const code = err.code && Number.isInteger(err.code) ? err.code : 500;
    return res.status(code).json({ error: "stash failed", details: err.message || String(err) });
  }
});

// Serve local hub-data if present
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
