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

const fs = require("fs");
const papaparse = require("papaparse");
const { Transform, Writable } = require("stream");
const csv = require("fast-csv");
const { getDomain: getDomainTldJS } = require("tldjs");
const path = require("path");
const Memcached = require("memcached");
const util = require("util");
const exec = util.promisify(require("child_process").exec);
const pLimit = require("p-limit");

const limit = pLimit(1);

const betterqlite3 = require("better-sqlite3");

class SqliteContactCache {
  constructor(serverLocations, options) {
    this.db = betterqlite3(":memory:", options);
    this.db.pragma("journal_mode = WAL");
    this.db.exec(
      "CREATE TABLE IF NOT EXISTS contacts ( email TEXT, fileName TEXT, UNIQUE(email))"
    );
  }

  getEmail(email) {
    return new Promise((resolve, reject) => {
      try {
        const stmt = this.db.prepare("SELECT * FROM contacts WHERE email = ?");
        const contact = stmt.get(email);
        resolve(contact);
      } catch (error) {
        reject(error);
      }
    });
  }

  insertContact(email, fileName) {
    // Validate data before insertion
    if (!email && !fileName) {
      throw new Error("Missing required fields: email, fileName");
    }

    return new Promise((resolve, reject) => {
      try {
        const stmt = this.db.prepare(
          "INSERT INTO contacts (email, fileName) VALUES (?, ?)"
        );

        const result = stmt.run(email, fileName);

        if (!result.changes) {
          throw new Error("Unique constraint violation or insertion failed.");
        }

        resolve({ email, fileName });
      } catch (err) {
        reject(err);
      }
    });
  }

  flush() {
    return new Promise((resolve, reject) => {
      try {
        this.db.exec("DELETE FROM contacts");
        resolve();
      } catch (err) {
        reject(err);
      }
    });
  }

  // eslint-disable-next-line class-methods-use-this
  disconnect() {
    return new Promise((resolve, reject) => {
      try {
        resolve();
      } catch (err) {
        reject(err);
      }
    });
  }
}

class localObject {
  Obj = {};
  constructor() {}
  set(key, value) {
    this.Obj[key] = value;
  }
  get(key) {
    return this.Obj[key];
  }
  disconnect() {
    console.log(
      "Message for memory object not Memchache as it was not loaded!!!"
    );
  }
}

class MemeCache {
  constructor(Server_locations, options) {
    this.MemeCache = new Memcached(Server_locations, options);
  }

  get(key) {
    return new Promise((resolve, reject) => {
      this.MemeCache.get(key, (err, data) => {
        if (err) {
          reject(err);
        } else {
          resolve(data);
        }
      });
    });
  }

  set(key, value, ttl) {
    ttl = ttl || 86400;
    return new Promise((resolve, reject) => {
      this.MemeCache.set(key, value, ttl, (err) => {
        if (err) {
          reject(err);
        } else {
          resolve("seted");
        }
      });
    });
  }

  flush() {
    return new Promise((resolve, reject) => {
      this.MemeCache.flush((err) => {
        if (err) {
          reject(err);
        } else {
          resolve("flushed");
        }
      });
    });
  }
}

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

function processResults(category, writeStream, rowRec, counter) {
  counter[category] = counter[category] + 1;
  writeStream[category].write(rowRec);
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

function processFile(fileDetails, contactSqlite) {
  return new Promise((resolve, reject) => {
    const { folderPath, filepath, name: fileName, ext } = fileDetails;

    let papaParserReadStream = papaparse.parse(papaparse.NODE_STREAM_INPUT, {
      header: true,
      worker: true,
      download: true,
      skipEmptyLines: true,
      encoding: "utf8",
    });

    const readStream = fs.createReadStream(filepath, {
      highWaterMark: 50 * 1024,
    });
    let Tracker = { readCnt: 0, processCnt: 0, limit: 0 };

    readStream.pipe(papaParserReadStream);

    let { writerObject: writeStream, counter } = createFolderAndWriterObj(
      folderPath,
      fileName + ext
    );

    process.stdout.write("\tProcessing Data");

    papaParserReadStream.on("data", (_rowRec) => {
      streamPasue(readStream, Tracker);
      limit(async () => {
        Tracker.limit++;
        let rowRec = {};
        rowRec.reason = "";
        rowRec.current_email = "";
        rowRec = Object.assign(rowRec, _rowRec);

        //console.log(JSON.stringify(rowRec,null,''))
        counter["Total"] = counter["Total"] + 1;

        //This is only for local run and tracking
        if (counter["Total"] % 10000 == 0) {
          process.stdout.write(".");
        }
        //--------------------------------------------------------

        //Find Unique WebSite

        // //Check if website is missing
        // if(!rowRec.URL || rowRec.URL?.trim().length == 0){
        //   //write to Missing File
        //   rowRec.reason = "Missing website";
        //   processResults("MissingDomain",writeStream,rowRec,counter);
        //   streamResume(readStream,Tracker)
        //   return;
        // }

        // //Generate Domain from website
        // rowRec.domain = getDomainTldJS(rowRec.URL);

        // //Check if domain is correct
        // if(!rowRec.domain || rowRec.domain?.trim().length == 0){
        //   //write to Missing File
        //   rowRec.reason = "Incorrect Website"
        //   processResults("IncorrectData",writeStream,rowRec,counter);
        //   streamResume(readStream,Tracker)
        //   return;
        // }

        //Find Unique Emails

        //Check if website is missing
        if (!rowRec.current_email || rowRec.current_email?.trim().length == 0) {
          //write to Missing File
          rowRec.reason = "Missing Email Address";
          processResults("MissingData", writeStream, rowRec, counter);
          streamResume(readStream, Tracker);
          return;
        }

        //Check domain in localDB
        let localJsonObjData;

        try {
          localJsonObjData = await contactSqlite.getEmail(rowRec.current_email);
        } catch (e) {
          console.log(e);
        }

        if (localJsonObjData) {
          rowRec.reason = `Duplicate on Emai - ${rowRec.current_email} - from File ${localJsonObjData}`;
          //Found duplicate records, writing it to duplicate file
          processResults("Duplicate", writeStream, rowRec, counter);
          streamResume(readStream, Tracker);
          return;
        }

        //Write to Local DB
        try {
          await contactSqlite.insertContact(rowRec.current_email, fileName);
        } catch (e) {
          console.log(e);
        }

        processResults("Unique", writeStream, rowRec, counter);
        streamResume(readStream, Tracker);
        return;
      });
    });

    papaParserReadStream.on("error", (err) => {
      reject(err);
    });

    papaParserReadStream.on("end", () => {
      limit(async () => {
        while (Tracker.readCnt !== Tracker.processCnt) {
          await sleep(3000);
        }
        resolve(JSON.stringify(counter, null, ""));
      });
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
      files.push({ folderPath, filepath, name, ext });
  });
  return files;
};

async function init(folderPath) {
  //basic setup

  //Creating Temp Folder
  const tempFolderPath = path.join(folderPath, `/temp`);
  fs.mkdirSync(tempFolderPath, { recursive: true }, (err) => {
    if (err) throw err;
  });

  // const memcached = createLocalObj(tempFolderPath);
  const logs = fs.createWriteStream(tempFolderPath + "/counterLog.txt", {
    flags: "w",
  });

  let contactSqlite;

  try {
    // console.log("Loading memcached");
    // var isLoaded = await exec("memcached.exe -d start -p 11211 -m 1024")
    //   .then(() => {
    //     return true;
    //   })
    //   .catch((err) => {
    //     return false;
    //   });
    // if (isLoaded) {
    //   memcached = new MemeCache("localhost:11211");
    //   await memcached.flush();
    //   console.log("Memcached Loaded Sucessfully");
    // } else {
    //   console.log("Memcached loading failed, using local object");
    //   memcached = new localObject();
    // }

    try {
      contactSqlite = new SqliteContactCache();
      await contactSqlite.flush();
    } catch (err) {
      logger.error(err);
    }
  } catch (err) {
    console.log(err);
  }
  return { contactSqlite, logs };
}

async function initDuplicateCheck(folderPath) {
  const { contactSqlite, logs } = await init(folderPath);

  if (!contactSqlite) {
    console.log("Unable to load memcached/local object - aborting process");
    return false;
  }
  const files = getFiles(folderPath);

  var totoalFiles = files.length,
    countProcess = 0;

  if (files.length == 0) {
    console.log("No CSV File found in Folder " + folderPath);
    return;
  }

  for (let x in files) {
    let file = files[x];
    countProcess++;
    console.log("\n" + file.name);
    console.log(
      "\tStarted: " + " Total of " + countProcess + " / " + totoalFiles
    );
    let MissingCount = await processFile(file, contactSqlite);
    logs.write(`${file.name} - |Stats| - ${MissingCount}` + "\r\n");
    console.log("\n\tWriting Counts Completed");
    delete MissingCount;
  }
  console.log("\nProcessing of all files have been Completed");
  logs.close();
  // memcached.disconnect();
}

// Create a readable stream from the CSV file.
var folderPath = "/home/sukhi/Downloads/Email_Pattern/Email_Files/Test/";
// var folderPath = "C:/Users/nexsa/Downloads/Testing/PPP/File/TestingFile";
var FileCategoryType = ["MissingData", "IncorrectData", "Unique", "Duplicate"];
initDuplicateCheck(folderPath);
