// index.js

// Import the migrate script
const run = require('./mongodb/migrate');

// Call the migrate function on mongodb
run().catch(console.error);
