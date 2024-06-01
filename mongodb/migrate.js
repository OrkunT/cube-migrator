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
      // If the key starts with a dollar sign, replace it with '$_'
      let safeKey = key.startsWith('$') ? `$_${key.slice(1)}` : key;
      let newPath = path ? `${path}.${safeKey}` : safeKey;

      if (typeof obj[key] === 'object' && obj[key] !== null) {
        traverse(obj[key], newPath);
      } else if (typeof obj[key] === 'number') {
        measures.push(newPath);
      }
    }
  }

  traverse(doc);

  return measures;
}


// Updated addMeasures function to handle individual measures
function addMeasures(measures1, measures2) {
  let aggregatedMeasures = {};
  Object.keys(measures1).forEach((measure) => {
    aggregatedMeasures[measure] = {
      sum: (measures1[measure].sum || 0) + (measures2[measure].sum || 0),
      min: Math.min(measures1[measure].min || Infinity, measures2[measure].min || Infinity),
      max: Math.max(measures1[measure].max || -Infinity, measures2[measure].max || -Infinity),
      count: (measures1[measure].count || 0) + (measures2[measure].count || 0)
    };
  });
  return aggregatedMeasures;
}

const Buffer = require('buffer').Buffer;

/// Function to encode keys when saving to MongoDB
function encodeKeys(obj) {
  for (let key in obj) {
    if (obj.hasOwnProperty(key)) {
      let encodedKey = Buffer.from(key).toString('hex');
      if (encodedKey !== key) {
        obj[encodedKey] = obj[key];
        delete obj[key];
      }
      if (typeof obj[encodedKey] === 'object' && obj[encodedKey] !== null) {
        encodeKeys(obj[encodedKey]);
      }
    }
  }
}

// Function to decode keys when querying from MongoDB
function decodeKeys(obj) {
  for (let key in obj) {
    if (obj.hasOwnProperty(key)) {
      let decodedKey = Buffer.from(key, 'hex').toString('utf8');
      if (decodedKey !== key) {
        obj[decodedKey] = obj[key];
        delete obj[key];
      }
      if (typeof obj[decodedKey] === 'object' && obj[decodedKey] !== null) {
        decodeKeys(obj[decodedKey]);
      }
    }
  }
}

function flattenedExclusiveAggregatedDimensions(dimensions, combinedDimensions = null, selection = null, docNumber = 0, resetInterval = 1000) {
  if (selection === null) {
    selection = ["up","custom","cmp","sg"];   //random tags
  }

  let result = combinedDimensions ? JSON.parse(JSON.stringify(combinedDimensions)) : {};
  let tracking = result.tracking || [];
  let keysInCurrentDoc = [];

  function isDate(integer) {
    const date = new Date(integer);
    return !isNaN(date.getTime());
  }

  function generateCombinedKeys(dimension, path = '') {
    if (typeof dimension === 'string') {
      return [path ? `${path}.${dimension}` : dimension];
    } else if (typeof dimension === 'object') {
      let combinedKeys = [];
      for (let key in dimension) {
        if (selection.includes(key)) {
          let subKeys = generateCombinedKeys(dimension[key], path ? `${path}.${key}` : key);
          combinedKeys = combinedKeys.concat(subKeys);
        } else {
          let combinedKey = path ? `${path}.${key}` : key;
          result[combinedKey] = dimension[key];
          keysInCurrentDoc.push(combinedKey);
        }
      }
      return combinedKeys;
    } else if (typeof dimension === 'number') {
      let type = isDate(dimension) ? 'time' : 'number';
      let value = type === 'time' ? BigInt(dimension) : dimension;
      return [path ? {key: `${path}`, value: value, type: type} : {key: '', value: value, type: type}];
    }
    return [];
  }

  let combinedKeys = generateCombinedKeys(dimensions);
  combinedKeys.forEach(combinedKey => {
    if (typeof combinedKey === 'object') {
      if (result.hasOwnProperty(combinedKey.key)) {
        result[combinedKey.key].count++;
        result[combinedKey.key].sum += combinedKey.value;
        result[combinedKey.key].min = Math.min(result[combinedKey.key].min, combinedKey.value);
        result[combinedKey.key].max = Math.max(result[combinedKey.key].max, combinedKey.value);
        result[combinedKey.key].original = combinedKey.value; // Add original value
        result[combinedKey.key].type = combinedKey.type; // Add type
      } else {
        result[combinedKey.key] = {
          count: 1,
          sum: combinedKey.value,
          min: combinedKey.value,
          max: combinedKey.value,
          original: combinedKey.value, // Add original value
          type: combinedKey.type // Add type
        };
      }
      keysInCurrentDoc.push(combinedKey.key);
    } else if (typeof combinedKey === 'string') {
      if (result.hasOwnProperty(combinedKey)) {
        result[combinedKey].count++;
      } else {
        result[combinedKey] = {count: 1, type: 'string'};
      }
      keysInCurrentDoc.push(combinedKey);
    }
  });

  // Update tracking
  if (docNumber === 0 || docNumber % resetInterval === 0) {
    // Initialize tracking for the first document or every resetInterval documents
    tracking = [keysInCurrentDoc.map(() => '1').join('')];
  } else {
    // Update tracking for subsequent documents
    let trackingForCurrentDoc = tracking[0].split('').map((val, index) => {
      return keysInCurrentDoc.includes(Object.keys(result)[index]) ? '1' : '0';
    }).join('');
    tracking.push(trackingForCurrentDoc);
  }

  result.tracking = tracking;
  return result;
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

      // Use flattenedExclusiveAggregatedDimensions function
      let currentDocAggregated = flattenedExclusiveAggregatedDimensions(currentDoc, previousDoc,null,processedDocuments,1000);

      // Insert the current document with updated measures into the target collection
      let encodedEntry = JSON.parse(JSON.stringify(currentDocAggregated));
      encodeKeys(encodedEntry);
      await targetCollection.insertOne(encodedEntry);

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