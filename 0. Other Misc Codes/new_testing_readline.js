const { Console } = require('console');
const fs = require('fs');
const readline = require('readline');
const csv = require('fast-csv');
const { Transform } = require("stream");
var replaceStream = require('replacestream');
const Papa = require('papaparse');


// var fileStream = fs.createReadStream(`C:/Users/nexsa/Downloads/Testing/MilapSource/Delaware.csv`);
var fileStream = fs.createReadStream(`C:/Users/nexsa/Downloads/Testing/MilapSource/myFile.csv`,'utf8');


let cnt = 0;
let dataNew = "";

const rl = readline.createInterface({
    input: fileStream,
    crlfDelay: Infinity,
});

// const csvParaser = csv.parse({
//     objectMode:true,
//     headers:(headers) => {
//         headers.map((el, i, ar) => {
//             if (ar.indexOf(el) !== i) {
//                 headers[i] = `${el}_${i}`;
//             }
//         });
//         return headers;
//     },
//     trim: true,
// })

const csvParaser = Papa.parse(csvData, {
    header: true, // Treat the first row as headers
    skipEmptyLines: true, // Skip empty lines
    complete: (results) => {
      // CSV parsing is complete, and you can now work with the parsed data
      console.log(results.data);
    },
})
.on('data',(row)=>{
    cnt++;
    //data = row;
    console.log(cnt);
    dataNew = row
    //console.log(cnt,JSON.stringify(data,null," "))
})
.on('end', () => {
    //reslove('t');
    console.log("Ended")
})
.on('error', (err) => {
    console.log("---",JSON.stringify(dataNew,null," "))
    console.error("Error in file: " , err);
});



( async () => {
    console.log("Start ti readline");
    //fileStream.pipe(replaceStream(/\\"/g, "")).pipe(csvParaser);
    
    // var cnts = 0;
    // rl.on("line",(data)=>{
    //     cnts++;
    //     //console.log(cnt,data)
    //     data = data.replace(/\\"/g, "")
    //     //console.log("GGG---",data);
    //     csvParaser.write(data+"\n");
    // });
    // rl.on("close",()=>{
    //     console.log("Ended Streem")
    //     csvParaser.end();
    // })
    fileStream.pipe(csvParaser);
    console.log("Ended Read Line");
    //console.log("---",JSON.stringify(data,null," "))
})()