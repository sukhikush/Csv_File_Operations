const Memcached = require('memcached');

// Create a new Memcached client
const memcached = new Memcached('localhost:11211'); // Specify your Memcached server address and port here

// Set a value in Memcached


function set(key,value){
    return new Promise( (resolve,reject) => {
        try
        {
            console.log("fffd")
            memcached.set('key1', 'Hello, Memcached!', 10, (err) => {
                if (err) {
                    reject(`Error setting value in Memcached: ${err}`);
                }
                resolve('Value set successfully in Memcached');
            });
        }catch(eer){
            console.log(eer)
        }
    });
}


(async ()=>{

    try{
        console.log("Set Start");
        await set();
        console.log("Set END");
    }catch(eer){
        console.log(eer)
    }

    // Retrieve a value from Memcached
    memcached.get('key1', (err, value) => {
        if (err) {
        console.error('Error retrieving value from Memcached:', err);
        return;
        }
        console.log('Retrieved value from Memcached:', value);
    });
    
    // Delete a value from Memcached
    memcached.del('key1', (err) => {
        if (err) {
        console.error('Error deleting value from Memcached:', err);
        return;
        }
        console.log('Value deleted successfully from Memcached');
    });
    
    // Close the Memcached connection
    memcached.end();

})();





// const os = require('os');
// const Memcached = require('memcached');

// function makeid(length) {
//     let result = '';
//     const characters = 'ABCDEFGHIJKLMNOPQRSTUVWXYZabcdefghijklmnopqrstuvwxyz0123456789';
//     const charactersLength = characters.length;
//     let counter = 0;
//     while (counter < length) {
//         result += characters.charAt(Math.floor(Math.random() * charactersLength));
//         counter += 1;
//     }
//     return result;
// }

// function setMemcaheData(data,counter){
//     var key = data + counter;
//     var value = data;
//     memcached.set('key1', 'Hello, Memcached!', 10, (err) => {
//         if (err) {
//             console.error('Error setting value in Memcached:', err);
//             return;
//         }
//         console.log('Value set successfully in Memcached');
//     });
// }

// const used = process.memoryUsage().heapUsed / 1024 / 1024;
// console.log(`The script uses approximately at start ${Math.round(used * 100) / 100} MB`);
// const obj = {}
// for (let i = 0; i < 1000000; i++) {
//     let id = makeid(10);
//     setMemcaheData(id,i)
//     if(i%250000 === 0){
//         process.stdout.write('.')
//     }
// }
// const usedEnd = process.memoryUsage().heapUsed / 1024 / 1024;
// console.log(`The script uses approximately at end ${Math.round(usedEnd * 100) / 100} MB`);
// console.log(_.size(obj))