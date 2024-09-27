const fs = require('fs');
const path = require('path');
const async = require('async');
const csv = require('fast-csv');
const { Transform } = require("stream");
const { log } = require('console');
const readline = require('readline');


const saveHeadersDetails = async (fileDetails,data) => {
    var headerlog = fs.createWriteStream(fileDetails.folderPath+"/headerlog.txt",{flags:'a'});
    headerlog.write(`${data} \r\n`);
    headerlog.close();
    console.log("\tWriting - Headser Info");
};

function childPRocessFile(file,logs){
    return new Promise((resolve,reject)=>{
        const { fork } = require("child_process");

        let args = [file.filepath,file.name];
        const child = fork(path.join(__dirname,'file_Missing_Cnt_Child.js'), args);
        child.on("message", async (msg) => {
            await saveHeadersDetails(file,msg[1])
            logs.write(`${file.name} - |MissingCount| - ${msg[0]}`+"\r\n")
            console.log("\tWriting - Counts Stats");
        });

        child.on('error', function (err) {
            reject();
        })

        child.on("close", function (code) {
            resolve("Done Processing File");
        });
    });
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

async function startMissingRowCounter(folderPath) {

    //Create Write Stream to loigs
    var logs = fs.createWriteStream(folderPath+"/counterLog.txt",{ flags: 'w' });

    var files = getFiles(folderPath);

    var totoalFiles = files.length,countProcess = 0;
    for (let x in files){

        let file = files[x];
        countProcess++;
        console.log("\n"+file.name);
        console.log("\tStarted: " + " Total of " + countProcess + " / " + totoalFiles);
        try {
            const MissingCount = await childPRocessFile(file,logs);
            console.log(`\t${MissingCount}`);
        } catch (err) {
            console.log(`\t Failed ${err.message}`)                 
        }

    }
    console.log('\nCompleted all files oprocessing');
    logs.close();
}


var findMissingHeader = [];
const FILE_PROCESS_CONCURRENCY = 1;

startMissingRowCounter('C:/Users/nexsa/Downloads/DataProcessor/Processing/Milap Source');