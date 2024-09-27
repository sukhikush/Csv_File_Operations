/*

*/

const fs = require("fs");
const papaparse = require("papaparse");
const csv = require("fast-csv");
const path = require("path");
const _ = require("lodash");
// const pLimit = require("p-limit");

// const limit = pLimit(1);

let GlempObj = require("./outputfile/empObj.json");
let GlrevObj = require("./outputfile/revObj.json");

function createFolderAndWriterObj(FolderPath, FileName) {
  var writerObject = {},
    counter = {};

  for (let CatType of FileCategoryType) {
    //Make Directory
    var newFolder = path.join(FolderPath, `/processedFile/${CatType}`);

    fs.mkdirSync(newFolder, { recursive: true }, (err) => {
      if (err) throw err;
    });

    //Writer Stream
    writerObject[CatType] = csv.format({ headers: true, objectMode: true });
    writerObject[CatType].pipe(
      fs.createWriteStream(path.join(newFolder, `${CatType}_${FileName}`), {
        encoding: "utf-8",
      })
    );

    counter[CatType] = 0;
  }

  counter["Total"] = 0;

  return { writerObject, counter };
}

function processResults(category, writeStream, rowRec, counter, wraperFn) {
  counter[category] = counter[category] + 1;
  writeStream[category].write(rowRec, wraperFn);
}

function streamPasue(readStream, Tracker) {
  Tracker.readCnt++;
  if (Tracker.readCnt - Tracker.processCnt > 1) {
    readStream.pause();
  }
}

function streamResume(readStream, Tracker) {
  Tracker.processCnt++;
  if (Tracker.readCnt - Tracker.processCnt < 1) {
    readStream.resume();
  }
}

function processFile(fileDetails, processingColsName) {
  return new Promise((resolve, reject) => {
    const { folderPath, filepath, name: fileName, ext } = fileDetails;

    let papaParserReadStream = papaparse.parse(papaparse.NODE_STREAM_INPUT, {
      header: true,
      worker: true,
      download: true,
      skipEmptyLines: true,
      encoding: "utf8",
    });

    const readStream = fs.createReadStream(filepath,{highWaterMark:1024*2});
    let Tracker = { readCnt: 0, processCnt: 0 };

    readStream.pipe(papaParserReadStream);

    let counter = {};
    counter["Total"] = 0;
    counter["Present"] = 0;
    counter["Missing"] = 0;

    process.stdout.write("\tProcessing Data");

    papaParserReadStream.on("data", (rowRec) => {
      streamPasue(readStream, Tracker);
      //limit(async() => {

      counter["Total"] = counter["Total"] + 1;

      //This is only for local run and tracking
      if (counter["Total"] % 10000 == 0) {
        process.stdout.write(".");
      }
      //--------------------------------------------------------

      
      //Check for Manditory Fields and if numeric value
      var MissingCol = "",
        ProcessType = "";

      for (let headName in processingColsName) {
        var colName = processingColsName[headName];
        if (!rowRec[colName] || rowRec[colName].trim().length < 1) {
          MissingCol = MissingCol + colName + ", -1- " + (rowRec[colName]);
        } else {
          if (["employee", "revenu"].indexOf(headName) > -1) {
            //check for numeric data
            if (isNaN(rowRec[colName]) || parseFloat(rowRec[colName]) <= 0) {
              MissingCol = MissingCol + colName + ", -2-";
            }
          }
        }
      }

      if (!MissingCol) {
        //write to All Manditory Fields Build Stats
        var ind = rowRec[processingColsName["industry"]];
        var subIn = rowRec[processingColsName["subIndustry"]];
        var emp = getEmployeeRange(rowRec[processingColsName["employee"]]);
        var rev = getRevenueRange(rowRec[processingColsName["revenu"]]);
        var objEmpKey = `${ind.toLocaleLowerCase()}_${subIn.toLocaleLowerCase()}_${emp.toLocaleLowerCase()}=:=${rev.toLocaleLowerCase()}`;
        var objRevKey = `${ind.toLocaleLowerCase()}_${subIn.toLocaleLowerCase()}_${rev.toLocaleLowerCase()}=:=${emp.toLocaleLowerCase()}`;

        var t1 = GlempObj[objEmpKey] ?? 0;
        GlempObj[objEmpKey] = parseInt(t1) + 1;

        var t2 = GlrevObj[objRevKey] ?? 0;
        GlrevObj[objRevKey] = parseInt(t2) + 1;

        counter["Present"] = counter["Present"] + 1;
      } else {
        counter["Missing"] = counter["Missing"] + 1;
      }

      streamResume(readStream, Tracker);
      //});
    });

    papaParserReadStream.on("error", (err) => {
      //limit.clearQueue();
      reject(err);
    });

    papaParserReadStream.on("end", async () => {
      // writeStream = null;
      //limit(async () => {
        while (Tracker.readCnt !== Tracker.processCnt) {
          await sleep(3000);
          console.log(
            "waiting for process to completed",
            JSON.stringify(Tracker, null, " ")
          );
        }
        console.log("\n\tEnd function called", JSON.stringify(counter));
        resolve(JSON.stringify(counter, null, ""));
      //});
    });
  });
}

function sleep(ms) {
  return new Promise((res) => setTimeout(res, ms));
}

function getEmployeeRange(employee) {
  if (isNaN(employee)) return false;

  var temp = parseFloat(employee);
  if (temp >= 1 && temp <= 10) return "0 - 10";
  if (temp >= 11 && temp <= 50) return "11 - 50";
  if (temp >= 51 && temp <= 200) return "51 - 200";
  if (temp >= 201 && temp <= 500) return "201 - 500";
  if (temp >= 501 && temp <= 1000) return "501 - 1,000";
  if (temp >= 1001 && temp <= 5000) return "1,001 - 5,000";
  if (temp >= 5001 && temp <= 10000) return "5,001 - 10,000";
  return "10,000+";
}

function getRevenueRange(revenue) {
  if (isNaN(revenue)) return false;

  var temp = parseFloat(revenue);
  if (temp >= 1 && temp <= 1000000) return "0 - $1M";
  if (temp >= 1000001 && temp <= 10000000) return "$1M - $10M";
  if (temp >= 10000001 && temp <= 50000000) return "$10M - $50M";
  if (temp >= 50000001 && temp <= 100000000) return "$50M - $100M";
  if (temp >= 100000001 && temp <= 250000000) return "$100M - $250M";
  if (temp >= 250000001 && temp <= 500000000) return "$250M - $500M";
  if (temp >= 500000001 && temp <= 1000000000) return "$500M - $1B";
  if (temp >= 1000000001 && temp <= 10000000000) return "$1B - $10B";
  return "$10B+";
}

const getFiles = (folderPath) => {
  const files = [];
  fs.readdirSync(folderPath).forEach((filename) => {
    const name = path.parse(filename).name;
    const filepath = path.resolve(folderPath, filename);
    const ext = path.extname(filepath);
    const stat = fs.statSync(filepath);
    const isFile = stat.isFile();

    if (isFile && ext == ".csv")
      files.push({ folderPath, filepath, name, ext });
  });
  return files;
};

function checkIfAllHeadersPresent(
  fileData,
  fetchHeader,
  fileName,
  errorFileHeader
) {
  try {
    var fileDataLower = fileData.map((data) => data.toLowerCase().trim());
    var temp = "";
    for (let findHeader in fetchHeader) {
      if (
        fileDataLower.indexOf(
          fetchHeader[findHeader].toLocaleLowerCase().trim()
        ) < 0
      ) {
        temp = temp + fetchHeader[findHeader] + ",";
      }
    }
    if (temp.length > 0) {
      errorFileHeader.push(`File Name - ${fileName}   MissingHeader - ${temp}`);
    }
  } catch (e) {
    console.log(e);
  }
}

async function checkHeader(fileDetails, findHeader) {
  var errorFileHeader = [];
  const { folderPath, filepath, name: fileName, ext } = fileDetails;

  var createReadStream = fs.createReadStream(filepath,{highWaterMark:1024*2});

  await new Promise((res, rej) => {
    papaparse.parse(createReadStream, {
      header: true,
      download: false,
      skipEmptyLines: false,
      encoding: "utf8",
      step: function (results, parser) {
        checkIfAllHeadersPresent(
          results.meta.fields,
          findHeader,
          fileName,
          errorFileHeader
        );
        parser.abort();
        results = null;
        delete results;
      },
      complete: function (results) {
        results = null;
        delete results;
        res("Done");
      },
    });
  });

  return errorFileHeader;
}

async function initBuildingProcess(folderPath, processingColsName) {
  const files = getFiles(folderPath);

  var totoalFiles = files.length,
    countProcess = 0,
    isHeaderMissing = false;

  console.log("\n--------Check Header-----------\n");

  for (let x in files) {
    let file = files[x];
    countProcess++;
    console.log("\n" + file.name);
    console.log(
      "\tStarted: " + " Total of " + countProcess + " / " + totoalFiles
    );
    const MissingCount = await checkHeader(file, processingColsName);
    if (MissingCount.length > 0) {
      isHeaderMissing = true;
      console.log("\t\t" + MissingCount);
    } else {
      console.log("\t\t All headers present");
    }
  }

  console.log("\n------------END----------------\n");

  console.log("\n--------Extracing Data --------\n");

  const empJson = fs.createWriteStream("outputfile/empObj.json",{ flags: 'w' });
  const revJson = fs.createWriteStream("outputfile/revObj.json",{ flags: 'w' });
  const logs = fs.createWriteStream("outputfile/counterLog.txt",{ flags: 'w' });

  (totoalFiles = files.length), (countProcess = 0);
  let tempLogCnt = 0;

  if (!isHeaderMissing) {
    for (let x in files) {
      let file = files[x];
      tempLogCnt = 0;

      countProcess++;
      console.log("\n\n" + file.name);
      console.log(
        "\tStarted: " + " Total of " + countProcess + " / " + totoalFiles
      );
      const MissingCount = await processFile(file, processingColsName);

      logs.write(`${file.name} - |Stats| - ${MissingCount}`+"\r\n",()=>{
        tempLogCnt++;
      });

      process.stdout.write(
        `\t${tempLogCnt} -  Waiting for write`
      );

      while (tempLogCnt < 0 ) {
        await sleep(1000);
        process.stdout.write(".");
      }
    }

    empJson.write(JSON.stringify(GlempObj,null," "),()=>{
      tempLogCnt++;
    });
    
    revJson.write(JSON.stringify(GlrevObj,null," "),()=>{
      tempLogCnt++;
    });
  }

  console.log("\n\nCompleted all files oprocessing",tempLogCnt);
  empJson.close();
  revJson.close();
  logs.close();
}

// Create a readable stream from the CSV file.

var folderPath =
  "D:/Sachin_Folders/LinkedInUS_Data_Source_3/TestFile";
var processingColsName = {
  industry: "resultIndustry",
  subIndustry: "resultSubIndustry",
  employee: "resultEmployeeSize",
  revenu: "resultRevenue",
};

initBuildingProcess(folderPath, processingColsName);
