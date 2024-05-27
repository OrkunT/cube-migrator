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
  const logFile = 'app.log';
  const maxLogSize = 50 * 1024 * 1024; // 50MB
  const logStream = fs.createWriteStream(logFile, { flags: 'a' });
  try {
    logStream.write('Connecting to the server...\n');
    await client.connect();
    logStream.write('Connected to the server.\n');

    const db = client.db(dbName);
    const sourceCollection = db.collection(sourceCollectionName);
    const totalDocuments = await sourceCollection.countDocuments();
    logStream.write(`Total documents to process: ${totalDocuments}\n`);

    const cursor = sourceCollection.find({}).sort({ ts: 1 });
    let processedDocuments = 0;
    let previousDoc = null;

    while(await cursor.hasNext()) {
      const doc = await cursor.next();

      // Check the size of the log file
      //const stats = fs.statSync(logFile);
      //if (stats.size > maxLogSize) {
        // If the log file is larger than 50MB, clear it
      //  fs.writeFileSync(logFile, '');
      //  logStream = fs.createWriteStream(logFile, { flags: 'a' });
     // }

      logStream.write(`Processing document ${processedDocuments + 1} of ${totalDocuments}...\n`);

      let dimensions = extractDimensions(doc); 
      let measures = extractMeasures(doc); 

      logStream.write(`Dimensions: ${util.inspect(dimensions)}\n`);
      logStream.write(`Measures: ${util.inspect(measures)}\n`);

      let cubePipeline = cubes.createCube(dimensions, measures, targetCollectionName);

      // Add a $match stage to the beginning of the pipeline to filter for the current document
      if (previousDoc) {
        cubePipeline.unshift({ $match: { ts: { $gt: previousDoc.ts } } });
      }

      logStream.write(`Running aggregation pipeline for document ${processedDocuments + 1}...\n`);
      await db.collection(sourceCollectionName).aggregate(cubePipeline).toArray();
      logStream.write(`Finished processing document ${processedDocuments + 1}.\n`);
      
      processedDocuments++;
      previousDoc = doc;
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