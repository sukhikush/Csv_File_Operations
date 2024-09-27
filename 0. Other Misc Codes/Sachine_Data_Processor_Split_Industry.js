/*

*/

const fs = require("fs");
const papaparse = require("papaparse");
const csv = require("fast-csv");
const path = require("path");
const _ = require("lodash");
const { getDomain: getDomainTldJS } = require("tldjs");

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
  if (Tracker.readCnt - Tracker.processCnt <= 1) {
    readStream.resume();
  }
}

function searchEmailObj(Obj) {
  var value = Object.values(Obj);
  var index = value.findIndex((v) => /.+@.+\..+/.test(v));
  return index > -1 ? value[index] : "";
}

var lastRecord = '';

function processFile(fileDetails) {
  return new Promise((resolve, reject) => {
    const { folderPath, filepath, name, filename, ext } = fileDetails;

    let papaParserReadStream = papaparse.parse(papaparse.NODE_STREAM_INPUT, {
      header: true,
      worker: true,
      download: true,
      skipEmptyLines: true,
      encoding: "utf8",
    });

    const readStream = fs.createReadStream(filepath, {
      highWaterMark: 1024 * 2,
    });
    const csvFormat = csv.format({ headers: true, objectMode: true });
    const writeFile = fs.createWriteStream(
      `${folderPath}/processedFile/P_${filename}`,
      { flags: "w" }
    );

    csvFormat.pipe(writeFile);

    let Tracker = { readCnt: 0, processCnt: 0 };
      let printCount = 0;

    readStream.pipe(papaParserReadStream);

    let counter = {};
    counter["Total"] = 0;
    counter["Present"] = 0;
    counter["Missing"] = 0;

    process.stdout.write("\tProcessing Data");

    papaParserReadStream.on("data", (rowRec) => {
      streamPasue(readStream, Tracker);

      counter["Total"] = counter["Total"] + 1;

      //This is only for local run and tracking
      if (counter["Total"] % 10000 == 0) {
        process.stdout.write(".");
      }

      lastRecord = rowRec;
      var arrayInd = ["error","error","error"]
      try{
        arrayInd = JSON.parse(rowRec.industry_v2_ranked);

        rowRec.Industry_1 = arrayInd[0]?.[0] ?? '';
        rowRec.Sub_Industry_1 = arrayInd[0]?.[1] ?? '';

        rowRec.Industry_2 = arrayInd[1]?.[0] ?? '';
        rowRec.Sub_Industry_2 = arrayInd[1]?.[1] ?? '';

        rowRec.Industry_3 = arrayInd[2]?.[0] ?? '';
        rowRec.Sub_Industry_3 = arrayInd[2]?.[1] ?? '';

      }catch(err){
                
        rowRec.Industry_1 = rowRec.industry_v2_ranked;
        rowRec.Sub_Industry_1 = '';

        rowRec.Industry_2 = '';
        rowRec.Sub_Industry_2 = '';

        rowRec.Industry_3 = '';
        rowRec.Sub_Industry_3 = '';
      }
      

      csvFormat.write(rowRec, () => {
        streamResume(readStream, Tracker);
      });
    });

    papaParserReadStream.on("error", (err) => {
      reject(err);
    });

    papaParserReadStream.on("end", async () => {
      while (Tracker.readCnt !== Tracker.processCnt) {
        await sleep(3000);
        console.log(
          "waiting for process to completed",
          JSON.stringify(Tracker, null, " ")
        );
      }
      console.log("\n\tEnd function called", JSON.stringify(counter));
      resolve(JSON.stringify(counter, null, ""));
    });
  });
}

function sleep(ms) {
  return new Promise((res) => setTimeout(res, ms));
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
      files.push({ folderPath, filepath, name, ext, filename: name + ext });
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

  var createReadStream = fs.createReadStream(filepath, {
    highWaterMark: 1024 * 2,
  });

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

async function initBuildingProcess(folderPath) {
  const files = getFiles(folderPath);

  var totoalFiles = files.length,
    countProcess = 0;

  (totoalFiles = files.length), (countProcess = 0);

  let tempLogCnt = 0,
    fileRowCounter = 0;

  const logs = fs.createWriteStream("outputfile/counterLog.txt", {
    flags: "w",
  });

  let readFile, writeFile;
  for (let x in files) {
    let file = files[x];
    tempLogCnt = 0;
    fileRowCounter = 0;

    console.log("\n\n" + file.filename);
    console.log(
      "\tStarted: " + " Total of " + countProcess++ + " / " + totoalFiles
    );
    var processStats = await processFile(file);

    logs.write(`${file.name} - |Stats| - ${processStats}` + "\r\n");
    console.log("\tWriting Counts Completed");
    delete processStats;
  }
  console.log("\nProcessing of all files have been Completed");
  logs.close();
}

// Create a readable stream from the CSV file.

var folderPath = "D:/Sachin_Folders/Sachin/Extract";

initBuildingProcess(folderPath);
