const fs = require('fs');
const path = require('path');
const async = require('async');
const csv = require('fast-csv');
const { Transform } = require("stream");
const { log } = require('console');
const readline = require('readline');


const getFiles = (folderPath) => {
  const files = []
  fs.readdirSync(folderPath).forEach(filename => {
    const name = path.parse(filename).name;
    const filepath = path.resolve(folderPath, filename);
    const ext = path.extname(filepath);
    const stat = fs.statSync(filepath);
    const isFile = stat.isFile();

    if (isFile && ext == ".csv") files.push({ folderPath,filepath, name, ext });
  })
  return files
}


const headerDataCounter = (row,findObj,compName) => {
    if(findMissingHeader.length > 0){
        findMissingHeader.forEach(header => {
            if(typeof row[header] == 'undefined'){
                findObj[header] = "Not Found"
            }else{
                if(row[header].length == 0){
                    findObj[header] ??= 0;
                    findObj[header] = findObj[header] + 1;
                }
            }
        });
    }else{
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
    }
};

const saveHeadersDetails = async (fileDetails,headers,duplicateRec) => {
    //return new Promise((resolve, reject) => {
        var headerlog = fs.createWriteStream(fileDetails.folderPath+"/headerlog.txt",{flags:'a'});
        headerlog.write(`${fileDetails.name} -||- ${headers.toString()} -||- ${duplicateRec.toString()} \r\n`);
        headerlog.close();
       // resolve("Done writing headers");
    //});
};



const processFile = (fileDetails) => {
    const {folderPath,filepath,name,ext} = fileDetails;

    return new Promise(async (resolve, reject) => {

        var findObj = {startT:'',endT:'',proT:0,totalRec:0,dupCompCnt:0,uniqueCompCnt:0},compName = [];
        const fileStream = fs.createReadStream(filepath);

        const rl = readline.createInterface({
            input: fileStream,
            crlfDelay: Infinity,
        });

        const csvParaser = csv.parse({
            headers:(headers) => {
                var duplicateRec = [];
                headers.map((el, i, ar) => {
                    if (ar.indexOf(el) !== i) {
                        duplicateRec.push(el);
                        headers[i] = `${el}_${i}`;
                    }
                });
                console.log(`\tCapturing headers Completed`);
                process.stdout.write('\tProcessing Data');
                var status = saveHeadersDetails(fileDetails,headers,duplicateRec);
                return headers;
            },
        })
        .on('data',(row)=>{
            findObj["startT"] = new Date();
            findObj["totalRec"] = findObj["totalRec"] + 1;
            headerDataCounter(row,findObj,compName)
            if(findObj["totalRec"] % 25000 == 0){
             process.stdout.write(".");
            }
        })
        .on('end', () => {
            console.log("END !");
            compName = [];
            resolve(JSON.stringify(findObj,null,''))
        })
        .on('error', (err) => {
            console.error("Error in file: " + name, err);
            reject(err)
        });

        ctn = 0;
        rl.on('line', (line) => {
            var modData = line.replace(/\\"/g, "");
            csvParaser.write(modData +"\r\n");
        });
        rl.on('close',() => {
            csvParaser.end();
        });
    });
}

async function startMissingRowCounter(folderPath) {

    //Create Write Stream to loigs
    var logs = fs.createWriteStream(folderPath+"/counterLog.txt",{ flags: 'w' });

    var files = getFiles(folderPath);

    var totoalFiles = files.length,countProcess = 0;
    for (let x in files){
        let file = files[x];
        countProcess++;
        console.log("\n"+file.name);
        console.log("\tStarted: " + " Total of " + countProcess + " / " + totoalFiles);
        const MissingCount = await processFile(file);
        logs.write(`${file.name} - |MissingCount| - ${MissingCount}`+"\r\n")
        console.log("\tWriting Counts Completed");
    }
    console.log('\nCompleted all files oprocessing');
    logs.close();

    //Write unique company names
    //var compnayName = fs.createWriteStream(folderPath+"/UniqueCompanyName.txt",{ flags: 'w' });

}


var findMissingHeader = [];
const FILE_PROCESS_CONCURRENCY = 1;

startMissingRowCounter('C:/Users/nexsa/Downloads/UnZip');