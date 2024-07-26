const fs = require("fs");
const uuidv4 = require("uuid").v4;
const { MongoClient } = require("mongodb");
const minimist = require("minimist");

const { timing } = require("./helpers/timing");
const { memory } = require("./helpers/memory");
const { Metrics } = require("./helpers/metrics");
const { Product } = require("./product");

const DATABASE_NAME = "test-product-catalog";
const MONGO_URL = `mongodb://localhost:27017/${DATABASE_NAME}`;
const catalogUpdateFile = "updated-catalog.csv";

const { size } = minimist(process.argv.slice(2));
if (!size) {
  throw new Error("Missing 'size' parameter");
}

const mongoClient = new MongoClient(MONGO_URL, {
  useNewUrlParser: true,
  useUnifiedTopology: true,
});

async function main() {
  await mongoClient.connect();
  const db = mongoClient.db();

  await clearExistingData(db);

  await memory("Generate dataset", () =>
    timing("Generate dataset", () => generateDataset(db, size))
  );

  await mongoClient.close();
}

async function clearExistingData(db) {
  await db.dropDatabase();
  if (fs.existsSync(catalogUpdateFile)) {
    fs.rmSync(catalogUpdateFile);
  }
}

async function generateDataset(db, catalogSize) {
  const writeStream = fs.createWriteStream(catalogUpdateFile, { flags: "a" });
  writeCsvHeaders(writeStream);

  const metrics = Metrics.zero();
  const createdAt = new Date();
  const products = [];
  const batchSize = 1000;

  for (let i = 0; i < catalogSize; i++) {
    const product = generateProduct(i, createdAt);
    products.push(product);

    const updatedProduct = generateUpdate(product, i, catalogSize);
    metrics.merge(
      writeProductUpdateToCsv(writeStream, product, updatedProduct)
    );

    if (products.length === batchSize || i === catalogSize - 1) {
      await db.collection("Products").insertMany(products);
      products.length = 0;
    }

    const progressPercentage = (i * 100) / catalogSize;
    if (progressPercentage % 10 === 0) {
      console.debug(`[DEBUG] Processing ${progressPercentage}%...`);
    }
  }

  writeStream.end();
  logMetrics(catalogSize, metrics);
}

function writeCsvHeaders(writeStream) {
  writeStream.write(Object.keys(generateProduct(-1, null)).join(",") + "\n");
}

function generateProduct(index, createdAt) {
  return new Product(
    uuidv4(),
    `Product_${index}`,
    generatePrice(),
    createdAt,
    createdAt
  );
}

function generatePrice() {
  return Math.round(Math.random() * 1000 * 100) / 100;
}

const productEvent = {
  pDelete: 10, // probability of deleting the product
  pUpdate: 10, // probability of updating the product
  pAdd: 20, // probability of adding a new product
};

function generateUpdate(product, index, catalogSize) {
  const rand = Math.random() * 100; // float in [0; 100]
  if (rand < productEvent.pDelete) {
    // [0; pDelete[
    // Delete product
    return null;
  }
  if (rand < productEvent.pDelete + productEvent.pUpdate) {
    // [pDelete; pUpdate[
    // Update product
    return new Product(
      product._id,
      `Product_${index + catalogSize}`,
      generatePrice(),
      product.createdAt,
      new Date()
    );
  }
  if (rand < productEvent.pDelete + productEvent.pUpdate + productEvent.pAdd) {
    // [pUpdate; pAdd[
    // Add new product
    return generateProduct(index + catalogSize, new Date());
  }

  // Unchanged product
  return product; // [pAdd; 100]
}

function writeProductUpdateToCsv(writeStream, product, updatedProduct) {
  if (updatedProduct) {
    if (updatedProduct._id === product._id) {
      writeStream.write(updatedProduct.toCsv() + "\n");
      return updatedProduct.updatedAt !== updatedProduct.createdAt
        ? Metrics.updated()
        : Metrics.zero();
    } else {
      writeStream.write(product.toCsv() + "\n");
      writeStream.write(updatedProduct.toCsv() + "\n");
      return Metrics.added();
    }
  } else {
    return Metrics.deleted();
  }
}

function logMetrics(catalogSize, metrics) {
  console.info(`[INFO] ${catalogSize} products inserted in DB.`);
  console.info(`[INFO] ${metrics.addedCount} products to be added.`);
  console.info(
    `[INFO] ${metrics.updatedCount} products to be updated ${(
      (metrics.updatedCount * 100) /
      catalogSize
    ).toFixed(2)}%.`
  );
  console.info(
    `[INFO] ${metrics.deletedCount} products to be deleted ${(
      (metrics.deletedCount * 100) /
      catalogSize
    ).toFixed(2)}%.`
  );
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
