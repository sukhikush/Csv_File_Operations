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


function createFolderAndWriterObj(FolderPath,FileName,Tracker,makeFolder){
  
    var writerObject;

    //Make Directory
    var newFolder = path.join(FolderPath,`/splitFile/${FileName}`);

    if(makeFolder){
        fs.mkdirSync(newFolder, { recursive: true }, (err) => {
            if (err) throw err;
        });
    }

    //Writer Stream
    writerObject = csv.format({ headers: true, objectMode: true })
    writerObject.pipe(fs.createWriteStream(path.join(newFolder,`${FileName}_${Tracker.splitFileCnt}.csv`), { encoding: 'utf-8' }));
  
    return writerObject;
  };
  
  function streamPasue(readStream,Tracker){
    Tracker.readCnt ++;
    if((Tracker.readCnt-Tracker.processCnt) > 1){
      readStream.pause();
    };
  };
  
  function streamResume(readStream,Tracker){
    Tracker.processCnt++;
    if((Tracker.readCnt-Tracker.processCnt) < 1){
      readStream.resume();
    }
  };

function processFile(fileDetails){

    return new Promise((resolve,reject)=>{
  
      const {folderPath,filepath,name:fileName,ext} = fileDetails;
  
      let papaParserReadStream = papaparse.parse(papaparse.NODE_STREAM_INPUT,{
          header: hasHeaders,
          worker: true,
          download: true,
          skipEmptyLines: true,
          encoding: "utf8",
          escapeChar:'\\'
          //transform: function (value) { console.log(value); return (value); }
      });
  
      const readStream = fs.createReadStream(filepath,{ highWaterMark: 2 * 1024 });
      readStream.pipe(papaParserReadStream);
      
      let Tracker = {readCnt:0,processCnt:0,splitFileCnt:0,splitWriteRequestCnt:0,splitWriteCompleteCnt:0};
      let writeStream = createFolderAndWriterObj(folderPath,fileName,Tracker,true);
  
      process.stdout.write('\tProcessing Data');
      
      papaParserReadStream.on('data', (data) => {
        
        let rowRec = {}
        if(!hasHeaders){
            var cnt=0,t="";
            for (let head of csvHeaders){
                var t = data[cnt] ?? ''
                rowRec[head] = t;//.replaceAll(/["\\]/ig,'');
                cnt++;
            };
        }else{
            rowRec = data
        }
        
        // // console.log(rowRec)
        // console.log("---------")
        // console.log(papaparse.unparse([rowRec],{header: false}));
        // console.log("---------")
        
        streamPasue(readStream,Tracker);
        limit(async() => {
  
          //This is only for local run and tracking
              if(Tracker["readCnt"] % 10000 == 0){
                process.stdout.write(".");
              }
          //--------------------------------------------------------

            if(Tracker.splitWriteRequestCnt >= MaxSplitRecord){
                while(Tracker.splitWriteCompleteCnt !== Tracker.splitWriteRequestCnt){
                    await sleep(3000);
                    //console.log("Waiting - Write to Complete",JSON.stringify(Tracker,null," "));
                };
                writeStream.end();

                Tracker.splitWriteCompleteCnt = 0;
                Tracker.splitWriteRequestCnt = 0;
                Tracker.splitFileCnt++;

                writeStream = createFolderAndWriterObj(folderPath,fileName,Tracker,false);
            }

            // if (rowRec.unique == 'NQ-bsorrell@balchem.com-a1121807-9566-4ced-a81e-9b4c08d716d2-45473b94-99b7-428c-8dbc-d0968bc0b706'){
            //   console.log(data)
            //   console.log(rowRec);
            // }

            if( rowRec.Client_Name == 'Xin-Xin'){
              Tracker.splitWriteRequestCnt++;
              writeStream.write(rowRec,()=>{
                  Tracker.splitWriteCompleteCnt++;
                  streamResume(readStream,Tracker);
              });
            }else{
              streamResume(readStream,Tracker);
            }
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
          resolve(JSON.stringify(Tracker,null,''));
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

    //Creating Split Folder
    
  
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

    if(!hasHeaders && csvHeaders.length == 0){
        console.log("Please define header");
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

var folderPath = "C:/Users/nexsa/Downloads/UserRequest";
// var folderPath = "C:/Users/nexsa/Downloads/Testing/StreamTest"
var csvHeaders = ['Client_Name','Client_pseudonym','Project_Name','Project_deliveryDate','Project_CreatedAt','Account_Name','Account_WebSite','Account_Domain','unique','id','createdAt','createdBy','updatedAt','updatedBy','email','deliveryStatus','researchStatus','callingStatus','complianceStatus','complianceComments','prefix','firstName','middleName','lastName','street1','street2','city','state','country','zipCode','genericEmail','phone','directPhone','jobTitle','jobLevel','jobDepartment','nsId','zoomInfoContactId','linkedInUrl','screenshot','handles','website','comments','source','stage','functions','disposition','zb','gmailStatus','mailTesterStatus','custom1','custom2','custom3','custom4','custom5','custom6','custom7','custom8','custom9','custom10','custom11','custom12','custom13','custom14','custom15','emailDedupeKey','phoneDedupeKey','companyDedupeKey','label','gmailStatusDateAndTime','zbDateAndTime','phoneExtension','duplicateOf','mobile','emailNameDedupeKey','emailDomainDedupeKey','ClientId','ProjectId','AccountId'];
var hasHeaders = false;
var MaxSplitRecord = 3000000;
initQualityCheck(folderPath);

