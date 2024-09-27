const { v4: uuidv4 } = require('uuid');

// Number of UUIDs to generate
const numUuids = 1050;

// Array to store the generated UUIDs
const uuidList = [];

// Generate UUIDs and store them in the array
for (let i = 0; i < numUuids; i++) {
//   uuidList.push(uuidv4());
    console.log(uuidv4());
}
