/*

*/

const fs = require('fs');
const papaparse = require('papaparse');
const csv = require("fast-csv");
const path = require('path');
const _ = require("lodash")


function createFolderAndWriterObj(FolderPath,FileName){
  
  var writerObject;

  for(let CatType of FileCategoryType){
    //Make Directory
    var newFolder = path.join(FolderPath,`/processedFile/`);

    fs.mkdirSync(newFolder, { recursive: true }, (err) => {
      if (err) throw err;
    });

    //Writer Stream
    writerObject = csv.format({ headers: true, objectMode: true });
    writerObject.pipe(fs.createWriteStream(path.join(newFolder,`${FileName}`), { encoding: 'utf-8' }));
  }

  return writerObject;
};

function streamPasue(readStream,readCnt,processCnt){
  readCnt ++;
  if((readCnt-processCnt) > 5){
    readStream.pause();
  };
};

function streamResume(readStream,readCnt,processCnt){
  processCnt++;
  if((readCnt-processCnt) < 1){
    readStream.resume();
  }
};

function processFile(fileDetails,extractHeaderData){
  
  const {folderPath,filepath,name:fileName,ext} = fileDetails;

  var extractHeaderDataCamel = extractHeaderData.map(data => _.camelCase(data));
  
  var papaParserReadStream = papaparse.parse(papaparse.NODE_STREAM_INPUT,{
      header: true,
      worker: true,
      download: true,
      skipEmptyLines: true,
      encoding: "utf8",
      transformHeader:function(h) {
        return _.camelCase(h);
      }
  });

  let readCnt = 0;processCnt = 0;
  const readStream = fs.createReadStream(filepath);

  readStream.pipe(papaParserReadStream);

  const writeStream = createFolderAndWriterObj(folderPath,fileName+ext);

  var totalrec = 0;
  process.stdout.write('\tProcessing Data');

  return new Promise((resolve,reject)=>{
    papaParserReadStream.on('data', (rowRec) => {
      
      streamPasue(readStream,readCnt,processCnt);

      totalrec ++;
      if(totalrec % 25000 == 0){
        process.stdout.write(".");
      }

      var newObj = _.pick(rowRec,extractHeaderDataCamel);
      writeStream.write(newObj);

      streamResume(readStream,readCnt,processCnt)
    });

    papaParserReadStream.on("error",(err)=>{
      reject(err)
    });

    papaParserReadStream.on("end",()=>{
      resolve("Processed: " + totalrec)
    });
  });
};


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

function checkIfAllHeadersPresent(fileData,fetchHeader,fileName,errorFileHeader){
  try{
    var fileDataLower = fileData.map(data => data.toLowerCase().trim());
    var temp = "";
    for(let findHeader of fetchHeader){
      if(fileDataLower.indexOf(findHeader.toLocaleLowerCase().trim()) < 0){
        temp = temp + findHeader + ",";
      }
    }
    if(temp.length > 0){
      errorFileHeader.push(`File Name - ${fileName}   MissingHeader - ${temp}`)
    }
  }catch(e){
    console.log(e)
  }
};


async function checkHeader(fileDetails,findHeader){

  var errorFileHeader = [];
  const {folderPath,filepath,name:fileName,ext} = fileDetails;

  var createReadStream = fs.createReadStream(filepath);

  await new Promise((res,rej)=>{
    papaparse.parse(createReadStream,{
      header: true,
      download: true,
      skipEmptyLines: true,
      encoding: "utf8",
      step: function(results, parser) {
          checkIfAllHeadersPresent(results.meta.fields,findHeader,fileName,errorFileHeader)
          parser.abort(); 
          results=null;   
          delete results;
      }, complete: function(results){
        results=null;
        delete results;
        res("Done");
      }
    });
  });

  return errorFileHeader;
}

async function initExtractionProcess(folderPath,extractHeaderData) {

  const files = getFiles(folderPath);

  var totoalFiles = files.length,countProcess = 0,isHeaderMissing = false;

  console.log("\n--------Check Header-----------\n");

  for (let x in files){
      let file = files[x];
      countProcess++;
      console.log("\n"+file.name);
      console.log("\tStarted: " + " Total of " + countProcess + " / " + totoalFiles);
      const MissingCount = await checkHeader(file,extractHeaderData);
      if(MissingCount.length > 0){
        isHeaderMissing = true;
      }
      console.log("\t\t"+MissingCount)
  }

  console.log("\n------------END----------------\n");
  console.log("\n--------Extracing Data --------\n");

  totoalFiles = files.length,countProcess = 0;
  if(!isHeaderMissing){
    for (let x in files){
        let file = files[x];
        countProcess++;
        console.log("\n"+file.name);
        console.log("\tStarted: " + " Total of " + countProcess + " / " + totoalFiles);
        const MissingCount = await processFile(file,extractHeaderData);
        console.log("\n\t"+MissingCount);
    }
  }
  console.log('\nCompleted all files oprocessing');
}

// Create a readable stream from the CSV file.
var folderPath = "C:/Users/nexsa/Downloads/Testing/PPP/File/TestingFile/Avention/MemcoryCache/processedFile/Unique";
var FileCategoryType = ["File Name","Company Name","City","Country","D&B Hoovers Industry","Employees","Revenue (USD)","US SIC 1987 Code","US SIC 1987 Description","NAICS 2012 Code","NAICS 2012 Description","URL","domain"];
initExtractionProcess(folderPath,FileCategoryType);


