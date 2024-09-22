const fs = require('fs');
const readline = require('readline');
const path = require('path');
const async = require('async');
var replaceStream = require('replacestream')

const SplitFileLineByLine = async (fileDetails, folderPath, batchSize, readyOnlyTop) => {

  const { filepath, name, ext } = fileDetails;
  const writeFolder = `${folderPath}/splitFiles`;

  const fileStream = fs.createReadStream(filepath);
  const rl = readline.createInterface({
    input: fileStream.pipe(replaceStream(/\\"/g, "")).pipe(replaceStream(/\\n/g, "")),
    crlfDelay: Infinity,
  });

  //

  fs.mkdirSync(writeFolder, { recursive: true }, (err) => {
    if (err) throw err;
  });

  var ctn = 0, flNameCtn = 1;
  var writeStr = fs.createWriteStream(`${writeFolder}/${name}_${flNameCtn}.csv`, { flags: 'w' });
  var header = '';
  console.log("Lopping via data")
  for await (const line of rl) {
    if (ctn > 0 && ctn % batchSize == 0) {
      if (readyOnlyTop) {
        break;
      }
      updateWritePointer(header)
    }
    if (ctn == 0) {
      header = line;
    }
    writeStr.write(line + "\r\n");
    ctn++;
  }
  console.log("For Loop completed")

  return flNameCtn;

  function updateWritePointer(header) {
    flNameCtn++;
    writeStr.close()
    writeStr = "";
    writeStr = fs.createWriteStream(`${writeFolder}/${name}_${flNameCtn}.csv`, { flags: 'w' });
    writeStr.write(header + "\r\n");
  }
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
  console.log(files)
  async.eachLimit(files, FILE_PROCESS_CONCURRENCY, async (file) => {
    var numberOfFiles = await SplitFileLineByLine(file, folderPath, batchSize, readyOnlyTop);
    console.log(`${file.name} - splited into - ${numberOfFiles} files`);
  });
}

startSplitProcess('/home/sukhi/Downloads/Vonage_Files/', 1000, true);
