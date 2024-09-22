const fs = require('fs');
const readline = require('readline');
const path = require('path');
const async = require('async');
var replaceStream = require('replacestream')

const SplitFileLineByLine = async (fileDetails, batchSize, readyOnlyTop) => {

  const { filepath, name, ext } = fileDetails;

  const fileStream = fs.createReadStream(filepath);
  const rl = readline.createInterface({
    input: fileStream.pipe(replaceStream(/\\"/g, "")).pipe(replaceStream(/\\n/g, "")),
    crlfDelay: Infinity,
  });


  var ctn = 0, flNameCtn = 1;

  var header = '';

  for await (const line of rl) {
    if (ctn > 0 && ctn % batchSize == 0) {
      if (readyOnlyTop) {
        break;
      }
      updateWritePointer(header)
    }
    if (ctn == 0) {
      header = `${name}.${ext},${line}`;
      console.log(header);
    } else {
      break;
    }
    ctn++;
  }

  return flNameCtn;
}

const getFiles = (folderPath) => {
  const files = []

  fs.readdirSync(folderPath).forEach(filename => {
    const name = path.parse(filename).name;
    const filepath = path.resolve(folderPath, filename);
    const ext = path.extname(filepath);
    const stat = fs.statSync(filepath);
    const isFile = stat.isFile();

    if (isFile && ext == ".csv") files.push({ filepath, name, ext });
  })

  return files
}


const FILE_PROCESS_CONCURRENCY = 10;

async function startSplitProcess(folderPath, batchSize, readyOnlyTop) {
  var files = getFiles(folderPath);
  async.eachLimit(files, FILE_PROCESS_CONCURRENCY, async (file) => {
    var numberOfFiles = await SplitFileLineByLine(file, batchSize, readyOnlyTop);
    // console.log(`${file.name} - splited into - ${numberOfFiles} files`);
  });
}

startSplitProcess('/home/sukhi/Downloads/Vonage_Files/', 1, true);
