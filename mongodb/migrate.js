// This script converts a MongoDB collection of documents into a hyperdimensional cube using a bottom-up approach.

const MongoClient = require('mongodb').MongoClient;
const cubes = require("mdb-cubes");

// Connection URL
const url = 'mongodb://mongodb:27017';

// Database Name
const dbName = 'testDB';

// Collection Names
const sourceCollectionName = 'testcollection';
const targetCollectionName = 'hypertestcollection';

// Create a new MongoClient
const client = new MongoClient(url);

function extractDimensions(doc) {
  let dimensions = [];

  function traverse(obj, path = '') {
    for (let key in obj) {
      if (typeof obj[key] === 'object' && obj[key] !== null) {
        traverse(obj[key], path ? `${path}.${key}` : key);
      } else if (typeof obj[key] === 'string') {
        dimensions.push(path ? `${path}.${key}` : key);
      }
    }
  }

  traverse(doc);

  return dimensions;
}

function extractMeasures(doc) {
  let measures = [];

  function traverse(obj, path = '') {
    for (let key in obj) {
      if (typeof obj[key] === 'object' && obj[key] !== null) {
        traverse(obj[key], path ? `${path}.${key}` : key);
      } else if (typeof obj[key] === 'number') {
        measures.push(path ? `${path}.${key}` : key);
      }
    }
  }

  traverse(doc);

  return measures;
}


async function run() {
  try {
    // Connect the client to the server
    await client.connect();
    // ... rest of your connection code

    const db = client.db(dbName);
    const sourceCollection = db.collection(sourceCollectionName);
    const totalDocuments = await sourceCollection.countDocuments();
    const cursor = sourceCollection.find({});
    let processedDocuments = 0;

    while(await cursor.hasNext()) {
      const doc = await cursor.next();

      // Extract dimensions and measures from the document
      let dimensions = extractDimensions(doc); 
      let measures = extractMeasures(doc); 

      // Create a cube for the document using the createCube function
      let cubePipeline = cubes.createCube(dimensions, measures, targetCollectionName);

      // Run the aggregation pipeline to create the cube
      await db.collection(sourceCollectionName).aggregate(cubePipeline).toArray();

      // Increment the counter for the documents processed
      processedDocuments++;
      
    }
  } finally {
    await client.close();
  }
}


module.exports = run;