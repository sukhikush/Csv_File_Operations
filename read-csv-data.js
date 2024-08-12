const fs = require('fs')
const csv = require('fast-csv')
// let techObject = require('../tech.json')

// console.log(techObject[Object.entries(techObject)[0]])

// Define the CSV file path (replace with the actual path to your file)
const csvFilePath =
  '/home/sukhi/Downloads/Account_Master_50+_/Data/Account_Master_Tech.csv'

let T = async () => {
  try {
    const techObject = {}

    // // Create a readable stream from the CSV file
    // const readStream = fs.createReadStream(csvFilePath);

    // // Parse the CSV stream using fast-csv
    // const csvStream = fastCsv.parseStream(readStream, options);

    // // Handle errors during parsing
    // csvStream.on("error", (error) => {
    //   console.error("Error reading CSV file:", error);
    //   process.exit(1); // Exit with an error code
    // });

    // Process each data row as it's parsed
    fs.createReadStream(csvFilePath)
      .pipe(csv.parse({ headers: true }))
      .on('data', row => {
        if (row.technology) {
          if (row.employeeRange == '0 - 10' || row.employeeRange == '11 - 50') {
            //skiping
          } else {
            let arrayData = row.technology.split(',')
            arrayData.forEach(element => {
              element = element.trim()
              techObject[element] ??= 0
              techObject[element]++
            })
          }
        }
      })
      .on('error', error => {
        console.error('Error reading CSV file:', error)
        process.exit(1) // Exit with an error code
      })
      .on('end', () => {
        console.log(techObject)
      })
  } catch (error) {
    console.error('Unexpected error:', error)
    process.exit(1) // Exit with an error code
  }
}

;(() => {
  T()
})()
