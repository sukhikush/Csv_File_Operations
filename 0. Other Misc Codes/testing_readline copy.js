const { Console } = require('console');
const fs = require('fs');
const readline = require('readline');
const csv = require('fast-csv');
const { Transform } = require("stream");

var fileStream = fs.createReadStream(`C:/Users/nexsa/Downloads/Testing/Wyoming.csv`);

const transform = new Transform({
    decodeStrings: false,
    transform: function (chunk, encoding, done) {
      let modData = chunk.toString();
    modData = modData.replace(/\\\n/g, "")
      modData = modData.replace(/\\"/g, "");
      done(null, modData);
    },
});

const rl = readline.createInterface({
    input: fileStream.pipe(transform),
    crlfDelay: Infinity,
});


function processLine(header,line) {

    console.log("in Function 1")
    return new Promise((reslove,reject)=>{

        console.log("in Function 2")

        const csvParaser = csv.parse({
            headers:(headers) => {
                headers.map((el, i, ar) => {
                    if (ar.indexOf(el) !== i) {
                        headers[i] = `${el}_${i}`;
                    }
                });
                return headers;
            },
        })
        .on('data',(row)=>{
            
        })
        .on('end', () => {
            reslove('t');
        })
        .on('error', (err) => {
            console.error("Error in file: " , err);
        });
        csvParaser.write(`${header}\n`);
        csvParaser.write(`${line}\n`);
        csvParaser.end();
    });
}



( async () => {
    console.log("Start ti readline");

    var cnt = 0;
    var header = '';
    for await (const line of rl) {
        try {
            if (cnt > 0){
                await processLine(header,line);
            }else{
                header = line;
                console.log("HHHH =>",line);
                cnt++;
            }
            //csvParaser.write(`${line}\n`);
        } catch (error) {
            console.log(error)
        }
    }

    console.log("Ended Read Line");
    //fileStream.pipe(transform).pipe(rl).pipe(csvParaser);
})()