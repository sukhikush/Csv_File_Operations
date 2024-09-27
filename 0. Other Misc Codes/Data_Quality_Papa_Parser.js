/*
  Code finds unique data using Papa - Parser
  Creates
    Missing File
    Incorrect Domain File
    Unique File
    Duplicate File

  Download Memcahe from
    https://github.com/jefyt/memcached-windows/releases/tag/1.6.8_mingw

    Start
      memcached.exe -d start -p 11211

*/

const fs = require('fs');
const papaparse = require('papaparse');
const { Transform, Writable } = require('stream');
const csv = require("fast-csv");
const { getDomain: getDomainTldJS } = require("tldjs");
const path = require('path');
const util = require('util');
const exec = util.promisify(require('child_process').exec);
const pLimit = require('p-limit');


const limit = pLimit(1);


function createFolderAndWriterObj(FolderPath,FileName){
  
  var writerObject = {}, counter = {};

  for(let CatType of FileCategoryType){
    //Make Directory
    var newFolder = path.join(FolderPath,`/processedFile/${CatType}`);

    fs.mkdirSync(newFolder, { recursive: true }, (err) => {
      if (err) throw err;
    });

    //Writer Stream
    writerObject[CatType] = csv.format({ headers: true, objectMode: true })
    writerObject[CatType].pipe(fs.createWriteStream(path.join(newFolder,`${CatType}_${FileName}`), { encoding: 'utf-8' }));

    counter[CatType] = 0;
  }

  counter["Total"] = 0;

  return {writerObject,counter};
};

function processResults(category,writeStream,rowRec,counter,wraperFn){
  counter[category] = counter[category] + 1;
  writeStream[category].write(rowRec,wraperFn);
};

function streamPasue(readStream,Tracker){
  Tracker.readCnt ++;
  if((Tracker.readCnt-Tracker.processCnt) > 1){
    readStream.pause();
  };
};

function streamResume(readStream,Tracker){
  Tracker.processCnt++;
  if((Tracker.readCnt-Tracker.processCnt) <= 1){
    readStream.resume();
  }
};

function processFile(fileDetails){

  return new Promise((resolve,reject)=>{

    const {folderPath,filepath,name:fileName,ext} = fileDetails;

    let papaParserReadStream = papaparse.parse(papaparse.NODE_STREAM_INPUT,{
        header: true,
        worker: true,
        download: true,
        skipEmptyLines: true,
        encoding: "utf8"
    });

    const readStream = fs.createReadStream(filepath);
    let Tracker = {readCnt:0,processCnt:0};

    readStream.pipe(papaParserReadStream);

    let {writerObject:writeStream,counter} = createFolderAndWriterObj(folderPath,fileName+ext,FileCategoryType);

    process.stdout.write('\tProcessing Data');
    
    papaParserReadStream.on('data', (rowRec) => {
      streamPasue(readStream,Tracker);
      limit(async() => {

        counter["Total"] = counter["Total"] + 1;

        //This is only for local run and tracking
            if(counter["Total"] % 10000 == 0){
              process.stdout.write(".");
            }
        //--------------------------------------------------------

        //Check for Manditory Fields
        var MissingCol = "",ProcessType = "";
        for(let colName of ManditoryColumn){
          if(!rowRec[colName] || rowRec[colName]?.trim().length < 1){
            MissingCol = MissingCol + colName + ", "
          }
        }

        if(!MissingCol){
            //write to All Manditory Fields
            ProcessType = "ManditoryFieldsPresent";
        }else{
          rowRec.reason = `Missing Manditory Fields - ${MissingCol}`;
          ProcessType = "MissingManditoryFields";
        }
        var wraperFn = ()=>{
          streamResume(readStream,Tracker);
        };

        processResults(ProcessType,writeStream,rowRec,counter,wraperFn);
        return;
      });
    });

    papaParserReadStream.on("error",(err)=>{
      limit.clearQueue();
      reject(err)
    });

    papaParserReadStream.on("end",()=>{
      // writeStream = null;
      limit(async() => {
        console.log("End function called",JSON.stringify(Tracker,null," "))
        while(Tracker.readCnt !== Tracker.processCnt){
          await sleep(3000);
          console.log("waiting for process to completed",JSON.stringify(Tracker,null," "))
        };
        resolve(JSON.stringify(counter,null,''));
      });
    });
  });
};

function sleep(ms) {
  return new Promise(res => setTimeout(res, ms));
}


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

async function init(folderPath){
  //basic setup

  //Creating Temp Folder
  const tempFolderPath = path.join(folderPath,`/temp`);
  fs.mkdirSync(tempFolderPath, { recursive: true }, (err) => {
    if (err) throw err;
  });

  const logs = fs.createWriteStream(tempFolderPath+"/counterLog.txt",{ flags: 'w' });

  return {logs}
}

async function initQualityCheck(folderPath) {

  //This init can be removed as it creates local logs
  const {logs} = await init(folderPath);

  //Getting Files list from Folder
  const files = getFiles(folderPath);

  var totoalFiles = files.length,countProcess = 0;
  
  if(files.length == 0){
    console.log("No CSV File found in Folder "+folderPath);
    return;
  }

  if(ManditoryColumn.length == 0){
    console.log("No Manditory Column found");
    return;
  }

  for (let x in files){
      let file = files[x];
      countProcess++;
      console.log("\n"+file.name);
      console.log("\tStarted: " + " Total of " + countProcess + " / " + totoalFiles);
      let processStats = await processFile(file);
      logs.write(`${file.name} - |Stats| - ${processStats}`+"\r\n")
      console.log("\n\tWriting Counts Completed");
      delete processStats;
  }
  console.log('\nProcessing of all files have been Completed');
  logs.close();
}

// Create a readable stream from the CSV file.
var folderPath = "C:/Users/nexsa/Downloads/Testing/PPP/File/TestingFile/Avention";
var FileCategoryType = ['ManditoryFieldsPresent',"MissingManditoryFields"];
var ManditoryColumn = ["Company Name","Employees","Revenue (USD)"];
initQualityCheck(folderPath);