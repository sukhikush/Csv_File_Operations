const fs = require('fs');
const path = require('path');
const async = require('async');
const csv = require('fast-csv');
const { Transform } = require("stream");
const { log } = require('console');
const readline = require('readline');
const { exit } = require('process');
var replaceStream = require('replacestream')





const headerDataCounter = (row,findObj,compName) => {
        for(let rec in row){
            findObj[rec] ??= 0;
            if(row[rec].length == 0){
                findObj[rec] = findObj[rec] + 1;
            }
        }
        //Logic to check duplicate compnay name
        if(typeof row["Company Name"] == 'undefined'){
            findObj["dupCompCnt"] = 'undefined';
            findObj["uniqueCompCnt"] = 'undefined';
        }else{
            if(row["Company Name"] == 0){
                findObj["nullCompCnt"] = findObj["nullCompCnt"] + 1;
            }else{
                if(compName.indexOf(row["Company Name"]) > -1){
                    findObj["dupCompCnt"] = findObj["dupCompCnt"] + 1;
                }else{
                    compName.push(row["Company Name"]);
                    findObj["uniqueCompCnt"] = findObj["uniqueCompCnt"] + 1;
                }
            }
        }
};





const processFile = (filepath,FileName) => {
    // const {folderPath,filepath,name,ext} = fileDetails;
    return new Promise(async (resolve, reject) => {
        var findObj = {processTime:0,totalRec:0,uniqueCompCnt:0,dupCompCnt:0,nullCompCnt:0},compName = [];
        var fileHeader = [],fileDuplicateHeaders = [];
        const start = process.hrtime(); // Get the start time


        const fileStream = fs.createReadStream(filepath);

        const csvParaser = csv.parse({
            delimiter:',',
            quote:'"',
            headers:(headers) => {
                headers.map((el, i, ar) => {
                    if (ar.indexOf(el) !== i) {
                        fileDuplicateHeaders.push(el);
                        headers[i] = `${el}_${i}`;
                    }
                });
                console.log(`\tCapturing headers Completed`);
                process.stdout.write('\tProcessing Data');

                fileHeader = [...headers];
                return headers;
            },
        })
        .on('data',(row)=>{
            findObj["totalRec"] = findObj["totalRec"] + 1;
            headerDataCounter(row,findObj,compName)
            if(findObj["totalRec"] % 25000 == 0){
                process.stdout.write(".");
            }
        })
        .on('end', () => {
            console.log(" END !");
            const end = process.hrtime();
            findObj["processTime"] = `${(end[0] - start[0]) + (end[1] - start[1]) / 1e9}`;

            var rep = [
                JSON.stringify(findObj,null,''),
                `${FileName} -||- ${fileHeader.toString()} -||- ${fileDuplicateHeaders.toString()}`
            ]
            resolve(rep);
        })
        .on('error', (err) => {
            console.error("Error in file: " + FileName,err);
            reject(err)
        });

        fileStream.pipe(replaceStream(/\\"/g, "")).pipe(csvParaser)
    });
}

(async ()=>{
    const filepath = process.argv[2];
    const name = process.argv[3];

    var data = await processFile(filepath,name);
    process.send(data);
})();