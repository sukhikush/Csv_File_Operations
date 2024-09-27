const fs = require('fs');
const readline = require('readline');


const getHeaders = async () => {
    var headers = "";
        
    const fileStream = fs.createReadStream("");
    const rl = readline.createInterface({
        input: fileStream,
        crlfDelay: Infinity,
    });
    
    for await (const line of rl) {
        headers = line;
        break;
    }
    return headers
};

(async() => {
    console.log("Start ---");
    await getHeaders();
    console.log("End ---");
})();


