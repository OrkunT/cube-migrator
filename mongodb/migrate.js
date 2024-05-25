// This script converts a MongoDB collection of documents into a hyperdimensional cube using a bottom-up approach.

const MongoClient = require('mongodb').MongoClient;
const cubes = require("mdb-cubes");

// Connection URL
const url = 'mongodb://localhost:27017';

// Database Name
const dbName = 'testDB';

// Collection Names
const sourceCollectionName = 'testcollection';
const targetCollectionName = 'hypertestcollection';

// Create a new MongoClient
const client = new MongoClient(url);

function extractDimensions(pipeline) {
  let dimensions = [];

  function traverse(obj, path = '') {
    for (let key in obj) {
      if (typeof obj[key] === 'object' && obj[key] !== null) {
        traverse(obj[key], path ? `${path}.${key}` : key);
      } else {
        dimensions.push(path ? `${path}.${key}` : key);
      }
    }
  }

  traverse(pipeline);

  return dimensions;
}

async function run() {
  try {
    // Connect the client to the server
    await client.connect();

    // Establish and verify connection
    await client.db("admin").command({ ping: 1 });
    console.log("Connected successfully to server");

    const db = client.db(dbName);

    // Get the source collection
    const sourceCollection = db.collection(sourceCollectionName);

    // Get the total number of documents in the collection
    const totalDocuments = await sourceCollection.countDocuments();

    // Get a cursor to the documents
    const cursor = sourceCollection.find({});

    // Initialize a counter for the documents processed
    let processedDocuments = 0;

    // Process documents in chunks
    while(await cursor.hasNext()) {
      const doc = await cursor.next();

      // Extract dimensions
      let dimensions = extractDimensions(doc);

      // Create a cube for the document
      let cube = cubes(doc);

      // Assign dimensions to the cube
      dimensions.forEach(dimension => {
        cube.dimension(dimension);
      });

      // Insert the cube into the target collection
      await db.collection(targetCollectionName).insertOne(cube);

      // Increment the counter for the documents processed
      processedDocuments++;

      // Print progress for every 10000 documents processed
      if (processedDocuments % 10000 === 0) {
        let progress = (processedDocuments / totalDocuments) * 100;
        console.log(`Processed ${processedDocuments} out of ${totalDocuments} documents (${progress.toFixed(2)}%)`);
      }
    }

  } finally {
    // Ensures that the client will close when you finish/error
    await client.close();
  }
}

module.exports = run;