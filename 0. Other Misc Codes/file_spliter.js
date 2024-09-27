const fs = require('fs');
const readline = require('readline');
const path = require('path');
const async = require('async');
var replaceStream = require('replacestream')

const SplitFileLineByLine = async (fileDetails,folderPath,batchSize) => {

  const {filepath,name,ext} = fileDetails;
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

  var ctn = 0,flNameCtn = 1;
  var writeStr = fs.createWriteStream(`${writeFolder}/${name}_${flNameCtn}.csv`,{ flags: 'w' });
  var header = '';
  for await (const line of rl) {
    if(ctn > 0 && ctn % batchSize == 0){
        updateWritePointer(header)
    }
    if(ctn == 0){
      header = line;
    }
    writeStr.write(line +"\r\n");
    ctn ++;
  }

  return flNameCtn;

  function updateWritePointer(header){
    flNameCtn ++;
    writeStr.close()
    writeStr = "";
    writeStr = fs.createWriteStream(`${writeFolder}/${name}_${flNameCtn}.csv`,{ flags: 'w' });
    writeStr.write(header+"\r\n");
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

async function startSplitProcess(folderPath,batchSize) {
  var files = getFiles(folderPath);
  async.eachLimit(files, FILE_PROCESS_CONCURRENCY, async (file) => {
    var numberOfFiles = await SplitFileLineByLine(file,folderPath,batchSize);
    console.log(`${file.name} - splited into - ${numberOfFiles} files`);
  });
}

startSplitProcess('C:/Users/nexsa/Downloads/Testing',2);
