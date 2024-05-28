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

function addMeasures(measures1, measures2) {
  return {
    sum: measures1.sum + measures2.sum,
    min: Math.min(measures1.min, measures2.min),
    max: Math.max(measures1.max, measures2.max),
    count: measures1.count + measures2.count
  };
}



const fs = require('fs');
const util = require('util');

async function run() {
  const logFile = 'app.log';
  const maxLogSize = 50 * 1024 * 1024; // 50MB
  let logStream = fs.createWriteStream(logFile, { flags: 'a' });
  try {
    logStream.write('Connecting to the server...\n');
    await client.connect();
    logStream.write('Connected to the server.\n');

    const db = client.db(dbName);
    const sourceCollection = db.collection(sourceCollectionName);
    const targetCollection = db.collection(targetCollectionName);
    const totalDocuments = await sourceCollection.countDocuments();
    logStream.write(`Total documents to process: ${totalDocuments}\n`);

    const cursor = sourceCollection.find({}).sort({ ts: 1 });
    let processedDocuments = 0;
    let previousDoc = null;

    while(await cursor.hasNext()) {
  const currentDoc = await cursor.next();

  // Check the size of the log file
  // ... (log file size check code)

  logStream.write(`Processing document ${processedDocuments + 1} of ${totalDocuments}...\n`);

  // Initialize current document with its own aggregated values
  let currentDocAggregated = {
    ...currentDoc,
    measures: extractMeasures(currentDoc) // Assuming extractMeasures returns the initial aggregated values
  };

  // If there is a previous document, retrieve its aggregated values and add them to the current document's measures
  if (previousDoc) {
    const previousDocInTarget = await targetCollection.findOne({ _id: previousDoc._id });
    currentDocAggregated.measures = addMeasures(currentDocAggregated.measures, previousDocInTarget.measures);
  }

  // Insert the current document with updated measures into the target collection
  await targetCollection.insertOne(currentDocAggregated);

  logStream.write(`Finished processing document ${processedDocuments + 1}.\n`);

  processedDocuments++;
  previousDoc = currentDocAggregated; // Update previousDoc to the current document for the next iteration
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