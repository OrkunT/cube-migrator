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


const fs = require('fs');
const util = require('util');

async function run() {
  const logStream = fs.createWriteStream('migrateapp.log', { flags: 'a' });
  try {
    logStream.write('Connecting to the server...\n');
    await client.connect();
    logStream.write('Connected to the server.\n');

    const db = client.db(dbName);
    const sourceCollection = db.collection(sourceCollectionName);
    const totalDocuments = await sourceCollection.countDocuments();
    logStream.write(`Total documents to process: ${totalDocuments}\n`);

    const cursor = sourceCollection.find({});
    let processedDocuments = 0;

    while(await cursor.hasNext()) {
      const doc = await cursor.next();

      logStream.write(`Processing document ${processedDocuments + 1} of ${totalDocuments}...\n`);

      let dimensions = extractDimensions(doc); 
      let measures = extractMeasures(doc); 

      logStream.write(`Dimensions: ${util.inspect(dimensions)}\n`);
      logStream.write(`Measures: ${util.inspect(measures)}\n`);

      let cubePipeline = cubes.createCube(dimensions, measures, targetCollectionName);

      logStream.write(`Running aggregation pipeline for document ${processedDocuments + 1}...\n`);
      await db.collection(sourceCollectionName).aggregate(cubePipeline).toArray();
      logStream.write(`Finished processing document ${processedDocuments + 1}.\n`);

      processedDocuments++;
    }

    logStream.write(`Finished processing all ${totalDocuments} documents.\n`);
  } finally {
    logStream.write('Closing the client connection...\n');
    await client.close();
    logStream.write('Client connection closed.\n');
    logStream.end();
  }
}


module.exports = run;