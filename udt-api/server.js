const express = require("express");
const { MongoClient } = require("mongodb");

const app = express();
app.use(express.json({ limit: "100mb" }));

const MONGO_URI = process.env.MONGO_URI;
const DB_NAME = process.env.DB_NAME || "udt_manager";
const API_KEY = process.env.API_KEY || "";

let db;

function requireKey(req, res, next) {
  if (!API_KEY) return next(); // allow if not set (not recommended)
  const key = req.header("X-API-Key");
  if (key !== API_KEY) return res.status(401).json({ ok: false, error: "Unauthorized" });
  next();
}

app.get("/health", async (req, res) => {
  try {
    await db.command({ ping: 1 });
    res.json({ ok: true });
  } catch (e) {
    res.status(500).json({ ok: false, error: String(e) });
  }
});

// Latest-only replace endpoint
app.post("/udt/latest/replace", requireKey, async (req, res) => {
  try {
    const { library, udtTypes, keys } = req.body;

    if (!library || !library.locationCode || !library.libraryName) {
      return res.status(400).json({ ok: false, error: "Missing library fields" });
    }

    const { locationCode, libraryName } = library;

    const libraries = db.collection("udt_libraries");
    const types = db.collection("udt_types");

    // 1) Upsert library metadata (latest)
    await libraries.updateOne(
      { locationCode, libraryName },
      { $set: { ...library, updatedAt: new Date() } },
      { upsert: true }
    );

    // 2) Delete removed UDTs (latest-only cleanup)
    await types.deleteMany({
      locationCode,
      libraryName,
      pathRel: { $nin: keys || [] }
    });

    // 3) Bulk upsert UDT docs
    if ((udtTypes || []).length > 0) {
      const bulk = types.initializeUnorderedBulkOp();
      for (const doc of udtTypes) {
        bulk
          .find({ locationCode, libraryName, pathRel: doc.pathRel })
          .upsert()
          .updateOne({ $set: { ...doc, updatedAt: new Date() } });
      }
      await bulk.execute();
    }

    res.json({ ok: true, upserted: (udtTypes || []).length });
  } catch (err) {
    res.status(500).json({ ok: false, error: String(err) });
  }
});

(async () => {
  if (!MONGO_URI) throw new Error("MONGO_URI env var is required");

  const client = new MongoClient(MONGO_URI);
  await client.connect();
  db = client.db(DB_NAME);

  // Indexes
  await db.collection("udt_libraries").createIndex(
    { locationCode: 1, libraryName: 1 },
    { unique: true }
  );
  await db.collection("udt_types").createIndex(
    { locationCode: 1, libraryName: 1, pathRel: 1 },
    { unique: true }
  );

  app.listen(8080, () => console.log("udt-api listening on :8080"));
})();
