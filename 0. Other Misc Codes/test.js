

const util = require('util');
const exec = util.promisify(require('child_process').exec);

( async()=>{
    try{
        await exec('memcached.exe -d start -p 11211')
    }catch(err){
        console.log(err)
    }
})();
