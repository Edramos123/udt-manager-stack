const express = require("express");
const { MongoClient } = require("mongodb");

const app = express();
app.use(express.json({ limit: "100mb" }));

const MONGO_URI = process.env.MONGO_URI;
const API_KEY = process.env.API_KEY || "";

// Comma-separated allowlist in docker-compose
const ALLOWED_DBS = new Set(
  (process.env.ALLOWED_DBS || "")
    .split(",")
    .map((s) => s.trim().toLowerCase())
    .filter(Boolean)
);

const PORT = parseInt(process.env.PORT || "8080", 10);

let client;

// ---- auth ----
function requireKey(req, res, next) {
  if (!API_KEY) return next(); // allow if not set
  const key = req.header("X-API-Key");
  if (key !== API_KEY) return res.status(401).json({ ok: false, error: "Unauthorized" });
  next();
}

// ---- validation ----
function normName(v) {
  if (v == null) return null;
  const s = String(v).trim().toLowerCase();
  // allow simple db/collection names only
  if (!/^[a-z0-9_]+$/.test(s)) return null;
  return s;
}

function getDbName(req) {
  // accept db from query or body
  const db = normName(req.query.db || req.body.db || req.body.dataset);
  if (!db) return { ok: false, error: "db is required (query param db=... or body db/dataset)" };
  if (ALLOWED_DBS.size > 0 && !ALLOWED_DBS.has(db)) {
    return { ok: false, error: "db not allowed", allowed: Array.from(ALLOWED_DBS) };
  }
  return { ok: true, db };
}

function getCollectionName(req) {
  const collection = normName(req.query.collection || req.body.collection || req.query.location || req.body.location);
  if (!collection) return { ok: false, error: "collection is required (query collection=... or body collection/location)" };
  return { ok: true, collection };
}

function mongoDb(dbName) {
  return client.db(dbName);
}

// ---- routes ----
app.get("/health", async (req, res) => {
  try {
    // Ping admin DB so health works even if a dataset DB doesn't exist yet
    await client.db("admin").command({ ping: 1 });
    res.json({ ok: true });
  } catch (e) {
    res.status(500).json({ ok: false, error: String(e) });
  }
});

/**
 * GET /v1/docs?db=udt_dc_dev&collection=dal&limit=200
 * Optional: &q=UA (searches name/_key)
 */
app.get("/v1/docs", requireKey, async (req, res) => {
  const dbRes = getDbName(req);
  if (!dbRes.ok) return res.status(400).json({ ok: false, error: dbRes.error, allowed: dbRes.allowed });

  const colRes = getCollectionName(req);
  if (!colRes.ok) return res.status(400).json({ ok: false, error: colRes.error });

  const limit = Math.min(parseInt(req.query.limit || "200", 10), 5000);
  const q = (req.query.q || "").toString().trim();

  try {
    const col = mongoDb(dbRes.db).collection(colRes.collection);

    const filter = q
      ? { $or: [{ name: { $regex: q, $options: "i" } }, { _key: { $regex: q, $options: "i" } }] }
      : {};

    const docs = await col.find(filter).sort({ _key: 1, name: 1 }).limit(limit).toArray();
    res.json({ ok: true, db: dbRes.db, collection: colRes.collection, count: docs.length, data: docs });
  } catch (e) {
    res.status(500).json({ ok: false, error: String(e) });
  }
});

/**
 * POST /v1/docs/latest/replace
 * Body:
 * {
 *   "db": "udt_dc_dev",
 *   "collection": "dal",
 *   "docs": [ ... ],
 *   "keys": [ ... ],              // optional; if omitted we derive from docs
 *   "keyField": "name"            // default "name"
 * }
 *
 * Behavior:
 * - Upsert each doc by _key (derived from doc[keyField])
 * - Delete any docs whose _key is NOT in keys (latest-only cleanup)
 */
app.post("/v1/docs/latest/replace", requireKey, async (req, res) => {
  const dbRes = getDbName(req);
  if (!dbRes.ok) return res.status(400).json({ ok: false, error: dbRes.error, allowed: dbRes.allowed });

  const colRes = getCollectionName(req);
  if (!colRes.ok) return res.status(400).json({ ok: false, error: colRes.error });

  const docs = req.body.docs || req.body.udtTypes || [];
  const keyField = (req.body.keyField || "name").toString();
  let keys = req.body.keys;

  if (!Array.isArray(docs)) {
    return res.status(400).json({ ok: false, error: "docs must be an array" });
  }

  // derive keys if not provided
  if (!Array.isArray(keys)) {
    keys = docs.map((d) => (d && d[keyField] != null ? String(d[keyField]) : null)).filter(Boolean);
  }

  try {
    const col = mongoDb(dbRes.db).collection(colRes.collection);

    // ensure index on _key for fast upsert + uniqueness
    await col.createIndex({ _key: 1 }, { unique: true });

    // 1) delete removed docs
    await col.deleteMany({ _key: { $nin: keys } });

    // 2) bulk upsert
    if (docs.length > 0) {
      const bulk = col.initializeUnorderedBulkOp();

      for (const d of docs) {
        const keyVal = d && d[keyField] != null ? String(d[keyField]) : null;
        if (!keyVal) continue;

        const stored = {
          ...d,
          _key: keyVal,
          updatedAt: new Date(),
          dataset: dbRes.db,
          location: colRes.collection
        };

        bulk
          .find({ _key: keyVal })
          .upsert()
          .updateOne({ $set: stored });
      }

      await bulk.execute();
    }

    res.json({
      ok: true,
      db: dbRes.db,
      collection: colRes.collection,
      upserted: docs.length,
      kept: keys.length
    });
  } catch (e) {
    res.status(500).json({ ok: false, error: String(e) });
  }
});

(async () => {
  if (!MONGO_URI) throw new Error("MONGO_URI env var is required");

  client = new MongoClient(MONGO_URI);
  await client.connect();

  app.listen(PORT, () => console.log(`udt-api listening on :${PORT}`));
})();
