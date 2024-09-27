const fs = require('fs');
const path = require('path');
const async = require('async');
const csv = require('fast-csv');
const { Transform } = require("stream");
const { log } = require('console');
const readline = require('readline');
const { exit } = require('process');





const headerDataCounter = (row,findObj,compName) => {
    // if(findMissingHeader.length > 0){
    //     findMissingHeader.forEach(header => {
    //         if(typeof row[header] == 'undefined'){
    //             findObj[header] = "Not Found"
    //         }else{
    //             if(row[header].length == 0){
    //                 findObj[header] ??= 0;
    //                 findObj[header] = findObj[header] + 1;
    //             }
    //         }
    //     });
    // }else{
        //Capture all header data
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
            if(compName.indexOf(row["Company Name"]) > -1){
                findObj["dupCompCnt"] = findObj["dupCompCnt"] + 1;
            }else{
                compName.push(row["Company Name"]);
                findObj["uniqueCompCnt"] = findObj["uniqueCompCnt"] + 1;
            }
        }
    // }
};





const processFile = (filepath,FileName) => {
    // const {folderPath,filepath,name,ext} = fileDetails;
    return new Promise(async (resolve, reject) => {

        var findObj = {processTime:0,totalRec:0,dupCompCnt:0,uniqueCompCnt:0},compName = [];
        var fileHeader = [],fileDuplicateHeaders = [];
        const start = process.hrtime(); // Get the start time


        const fileStream = fs.createReadStream(filepath);

        const transform = new Transform({
            decodeStrings: false,
            transform: function (chunk, encoding, done) {
              let modData = chunk.toString();
             //modData = modData.replace(/\\\n/g, "")
              //modData = modData.replace(/\\"/g, "");
              done(null, modData);
            },
        });

        const rl = readline.createInterface({
            input: fileStream.pipe(transform),
            crlfDelay: Infinity,
        });

        const waitCsvParser = (rlfileHeader,line,cnt) => {
            return new Promise((res,rej)=>{
                const csvParaser = csv.parse({
                    delimiter:',',
                    quote:'"',
                    headers:(headers) => {
                        headers.map((el, i, ar) => {
                            if (ar.indexOf(el) !== i) {
                                if(cnt < 2) fileDuplicateHeaders.push(el);
                                headers[i] = `${el}_${i}`;
                            }
                        });
                        if(cnt < 2){
                            fileHeader = [...headers];
                        }
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
                    res('read line')
                })
                .on('error', (err) => {
                    console.error("Error in file: " + FileName,err);
                    rej(err)
                });

                csvParaser.write(`${rlfileHeader}\n`);
                csvParaser.write(`${line}\n`);
                csvParaser.end();
            });
        }

        var rlfileHeader,cnt=0;
        for await (const line of rl) {
            try {
                if (cnt > 0){
                    var t = await waitCsvParser(rlfileHeader,line,cnt);
                }else{
                    rlfileHeader = line;
                    console.log(`\tCapturing headers Completed`);
                    process.stdout.write('\tProcessing Data');
                }
                cnt++
            } catch (error) {
                console.log("<===== Line ====>")
                console.log(line)
                console.log("<===== Line ====>")
                console.log("Error in file",error)
                break;
            }
        }

        console.log(" END !");
        //Send header,duplicateheader, fileObjecect
        const end = process.hrtime(); // Get the end time
        findObj["processTime"] = `${(end[0] - start[0]) + (end[1] - start[1]) / 1e9}`;

        var rep = [
            JSON.stringify(findObj,null,''),
            `${FileName} -||- ${fileHeader.toString()} -||- ${fileDuplicateHeaders.toString()}`
        ]
        resolve(rep);
    });
}

(async ()=>{
    const filepath = process.argv[2];
    const name = process.argv[3];

    var data = await processFile(filepath,name);
    process.send(data);
})();