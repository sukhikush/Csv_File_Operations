/*
Merge Files using ReadLine Interface
Guessm this code assumes that all files have same sequence if headers
*/
const path = require("path");
const fs = require("fs");
const readline = require("readline");

async function processLineByLine(res, rej, file) {}

const splitFileMaxRowCnt = 5 * 10000000;
let splitFileCurrentRowCnt = 0;
let headerData;
let splitFileCounter = 1;
const folderPath =
  "/home/sukhi/Downloads/Email_Pattern/Email_Files/Files/processedFile/Invalid/";
let writeHeader = true;
let writerObject;

function createWriterHead() {
  let t = fs.createWriteStream(
    path.join(
      folderPath,
      `/mergedFiles/Error-master-rl-${splitFileCounter++}.csv`
    )
  );
  return t;
}

function writeDataToFile(data) {
  writerObject.write(data + "\r\n");
  splitFileCurrentRowCnt++;
  if (splitFileCurrentRowCnt >= splitFileMaxRowCnt) {
    splitFileCurrentRowCnt = 0;
    writeHeader = true;
    writerObject.close();
    writerObject = null;
  }
}

(async () => {
  //Merged Folder
  fs.mkdirSync(
    path.join(folderPath, `/mergedFiles`),
    { recursive: true },
    (err) => {
      if (err) throw err;
    }
  );

  for (let file of fs.readdirSync(folderPath)) {
    const filepath = path.resolve(folderPath, file);
    const ext = path.extname(filepath);
    const stat = fs.statSync(filepath);
    const isFile = stat.isFile();

    if (isFile && ext == ".csv") {
      console.log(`Processing File ${file}`);
      await new Promise((res, rej) => {
        try {
          let linctn = 0;
          let rl = readline.createInterface({
            input: fs.createReadStream(path.join(folderPath, file)),
            crlfDelay: Infinity,
          });

          console.log("\tWriting Data");
          rl.on("line", (line) => {
            //some standard initialization
            // if (linctn == 0 && headerData) {
            //   if (headerData != line) {
            //     rl.pause();
            //     rl.off("line");
            //     rl.close();
            //     rl = null;
            //     console.log("innnn");
            //     rej("\tInvalid Data, header mismatch");
            //     return;
            //   }
            // }
            if (writeHeader) {
              headerData ??= line;
              writerObject = createWriterHead();
              writeDataToFile(headerData);
              writeHeader = false;
            }
            if (linctn++) writeDataToFile(line);
            //This is only for local run and tracking
            if (linctn % 10000 == 0) {
              process.stdout.write(".");
            }
          }).on("close", () => {
            res(`\tCompleted Writing Data`);
          });
        } catch (err) {
          rej(`\tError in processing ${file} detail ${err}`);
        }
      })
        .then((data) => {
          console.log(data);
        })
        .catch((err) => {
          console.log(err);
        });
      writeHeader = false;
    }
  }

  console.log("Merged All Files");
})();
