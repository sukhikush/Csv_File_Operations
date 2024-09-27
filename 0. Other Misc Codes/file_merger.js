const path = require('path');
const fs = require('fs');
const readline = require('readline');


(async ()=>{

    const fileStartsWith = 'urls_';
    const folderPath = path.join(path.resolve(),'processedFile');

    var writeStr = fs.createWriteStream(path.join(folderPath,"all_Merged.csv"));

    let writeHeader = true;
    for(let file of fs.readdirSync(folderPath)){
        await new Promise((res,rej)=>{
            processLineByLine(res,rej,file); 
        }).then((data)=>{
            console.log(data);
        }).catch((err)=>{
            console.log(err)
        });
        writeHeader = false
    }

    console.log("Merged All Files");


    async function processLineByLine(res,rej,file) {
        try{
            let rl = readline.createInterface({
                input: fs.createReadStream(path.join(folderPath,file)),
                crlfDelay: Infinity
            });
    
            let linctn = 0;
            
            console.log(`Processing File ${file}`);
    
            rl.on('line', (line) => {
                linctn++;
                (linctn == 1 && !writeHeader)?console.log('skiping'):writeStr.write(line+"\r\n");
            }).on('close', () => {
                res(`Completed PRocessing ${file}`);
            });
        }catch(err){
            rej(`Error in processing ${file} detail ${err}`)
        }
    }

})()