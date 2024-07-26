const fs = require("fs");
const readline = require("readline");
const { MongoClient } = require("mongodb");

const { memory } = require("./helpers/memory");
const { timing } = require("./helpers/timing");
const { Metrics } = require("./helpers/metrics");
const { Product } = require("./product");

const MONGO_URL = "mongodb://localhost:27017/test-product-catalog";
const catalogUpdateFile = "updated-catalog.csv";

async function main() {
  const mongoClient = new MongoClient(MONGO_URL);
  const connection = await mongoClient.connect();
  const db = connection.db();
  await memory("Update dataset", () =>
    timing("Update dataset", () => updateDataset(db))
  );
}

async function updateDataset(db) {
  const metrics = Metrics.zero();
  const products = new Map();
  const batchSize = 1000;

  const fileStream = fs.createReadStream(catalogUpdateFile, {
    encoding: "utf-8",
  });
  const rl = readline.createInterface({
    input: fileStream,
    crlfDelay: Infinity,
  });

  let isFirstLine = true;
  let lineCount = 0;

  for await (const line of rl) {
    // This is for skipping the header
    if (isFirstLine) {
      isFirstLine = false;
      continue;
    }
    // Skip empty lines
    if (line.trim() === "") continue;

    const product = Product.fromCsv(line);
    products.set(product._id, product);
    lineCount++;

    if (lineCount % 10000 === 0) {
      console.debug(`[DEBUG] Processed ${lineCount} rows...`);
    }
  }

  // Perform bulk write operations
  const bulkOps = Array.from(products.values()).map((product) => ({
    updateOne: {
      filter: { _id: product._id },
      update: { $set: product },
      upsert: true,
    },
  }));

  for (let i = 0; i < bulkOps.length; i += batchSize) {
    const batch = bulkOps.slice(i, i + batchSize);
    const result = await db.collection("Products").bulkWrite(batch);
    metrics.updatedCount += result.modifiedCount;
    metrics.addedCount += result.upsertedCount;
  }

  // Handle deletions
  let lastId = null;
  let deletedCount = 0;

  while (true) {
    const query = lastId ? { _id: { $gt: lastId } } : {};
    const existingIds = await db
      .collection("Products")
      .find(query, { projection: { _id: 1 } })
      .sort({ _id: 1 })
      .limit(batchSize)
      .toArray();

    if (existingIds.length === 0) break;

    const idsToDelete = existingIds
      .filter(({ _id }) => !products.has(_id))
      .map(({ _id }) => _id);

    if (idsToDelete.length > 0) {
      const deleteResult = await db
        .collection("Products")
        .deleteMany({ _id: { $in: idsToDelete } });
      deletedCount += deleteResult.deletedCount;
    }

    lastId = existingIds[existingIds.length - 1]._id;

    if (existingIds.length < batchSize) break;
  }

  metrics.deletedCount = deletedCount;

  logMetrics(lineCount, metrics);
}

function logMetrics(numberOfProcessedRows, metrics) {
  console.info(`[INFO] Processed ${numberOfProcessedRows} CSV rows.`);
  console.info(`[INFO] Added ${metrics.addedCount} new products.`);
  console.info(`[INFO] Updated ${metrics.updatedCount} existing products.`);
  console.info(`[INFO] Deleted ${metrics.deletedCount} products.`);
}

if (require.main === module) {
  main()
    .then(() => {
      console.log("SUCCESS");
      process.exit(0);
    })
    .catch((err) => {
      console.log("FAIL");
      console.error(err);
      process.exit(1);
    });
}
