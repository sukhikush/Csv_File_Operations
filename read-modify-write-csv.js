/*
This file reads csv file using fast-csv
Modify and write the csv file using fast-csv
*/

const csv = require("fast-csv");
const fs = require("fs");
const path = require("path");

//uat:[prod,useremail]
const mappingObj = {
  "e77ad557-4bb5-41a7-836c-ed69195845e4": [
    "996e0efe-a1d7-4c68-beac-d102b835c5ee",
    "milap.shah@nexsales.com",
  ],
  "c5db7e83-1de4-4160-9883-4ed22fb60c80": [
    "fa45de65-85e8-4f44-84a7-29dffaf9fa7f",
    "ritesh.lodaya@nexsales.com",
  ],
  "f5605c99-0a92-4270-9cee-2e3a3f7bf187": [
    "5b2b8084-a981-47a6-9423-b723051e5336",
    "nishit.gadhaiya@nexsales.com",
  ],
  "cf087ae8-4eef-41da-a5d5-928985e1cc66": [
    "d2a0b919-e12b-4e8e-ad25-ac2dc73ccdee",
    "sajid.khan@nexsales.com",
  ],
  "2118095a-ca43-4237-822c-e71ff6260d8b": [
    "61708c24-bfe4-42d5-b3db-452c3ccf9f3b",
    "rohit.bhat@nexsales.com",
  ],
  "b2b04d9f-daf6-423e-bdc2-8b4b170980de": [
    "d8e06db1-1383-4b6a-8476-4f7fbe033e08",
    "karthik.avudaiappan@nexsales.com",
  ],
  "93f456a4-a22e-41d5-ae6c-e66d052f14e3": [
    "577ee418-c402-4e55-a965-2b8dbbc1603e",
    "bgossett@responsepoint.com",
  ],
  "1d88e4b2-3d03-443e-9996-caac483d6971": [
    "fe0a3aef-58ad-414a-be42-ce89a974f62e",
    "vatsal.mistry@nexsales.com",
  ],
  "8a50d1db-8f89-4ae6-8126-d6cc8d09d6b9": [
    "ec9a3e2c-16fa-42ce-bb93-66fcfe3321b5",
    "vijay.pardeshi@nexsales.com",
  ],
  "0b80a36d-9626-494b-b896-8dd3ef6f884d": [
    "d575fab1-c552-4acd-852f-ae561cb85c59",
    "richard.pinto@nexsales.com",
  ],
  "6d1900f2-371d-4c8f-a983-bbfee37cd75d": [
    "8b426b2f-2ace-40bf-b2e1-51b7fd25753b",
    "rohit.khedekar@nexsales.com",
  ],
  "3fc2a230-5cf6-4357-9487-31b08899d977": [
    "8b3c42e8-2995-4b65-b20b-9e74b7bdc9fa",
    "jay.kamdar@nexsales.com",
  ],
  "81c9bd8c-017b-4ce6-a17b-aa6215de8b6b": [
    "cef3b278-b6fd-4abb-aa66-50c40fd7d88f",
    "stutika.pednekar@nexsales.com",
  ],
  "cb7e2281-f2a5-43e3-baa2-5a669d611d1b": [
    "5c1d8bb7-abed-4235-a28b-33dd945a4563",
    "poonam.shambharkar@nexsales.com",
  ],
  "17642751-19e9-4a8a-9794-89e1e5bfaedf": [
    "61177f72-010f-4ed7-bff7-eb47afc1441c",
    "akshata.potale@nexsales.com",
  ],
  "5b5ef4aa-c3f1-45ff-939a-9e35a5b444ac": [
    "5b5ef4aa-c3f1-45ff-939a-9e35a5b444ac",
    "bot@nexsales.com",
  ],
  "9441ec87-b4d8-4417-8f79-49760522bccd": [
    "61177f72-010f-4ed7-bff7-eb47afc1441c",
    "akshata.potale@nexsales.com",
  ], //["suraj.yadav@nexsales.com"],
  "3fb3b1fa-f647-4f8a-8d6a-c50a3de1bef2": [
    "d2a0b919-e12b-4e8e-ad25-ac2dc73ccdee",
    "sajid.khan@nexsales.com",
  ], //divya.trivedi@nexsales.com
};

const getFiles = (folderPath) => {
  const files = [];
  fs.readdirSync(folderPath).forEach((filename) => {
    const fileName = path.parse(filename).name;
    const filePath = path.resolve(folderPath, filename);
    const ext = path.extname(filePath);
    const stat = fs.statSync(filePath);
    const isFile = stat.isFile();

    if (isFile && ext == ".csv")
      files.push({ folderPath, filePath, fileName, ext });
  });
  return files;
};

function transformData(row) {
  // row.email = mappingObj[row.Email];
  if (row.name) row.name = row.name + " - UAT";
  if (row.created_by) row.created_by = mappingObj[row.created_by][0];
  if (row.updated_by) row.updated_by = mappingObj[row.updated_by][0];
  if (row.assigned_to) row.assigned_to = mappingObj[row.assigned_to][0];
  if (row.assigned_by) row.assigned_by = mappingObj[row.assigned_by][0];
  if (row.user_id) row.user_id = mappingObj[row.user_id][0];
}
function processFile({ folderPath, filePath, fileName, ext }) {
  return new Promise((resolve, reject) => {
    let rowCnt = 0;
    let curDat = {};
    process.stdout.write("\t-Processing ");
    fs.createReadStream(filePath)
      .pipe(csv.parse({ headers: true }))
      .on("data", (_row) => {
        curDat = _row;
        rowCnt++;
        transformData(_row);
        if (rowCnt % 1 == 0) {
          process.stdout.write(".");
        }
      })
      .on("end", () => {
        console.log("\n\t-CSV parsed successfully");
      })
      .on("error", (err) => {
        console.log(curDat);
        reject(err);
      })
      .pipe(csv.format({ headers: true }))
      .pipe(
        fs.createWriteStream(
          path.join(folderPath, `/processFiles/${fileName + ext}`)
        )
      )
      .on("finish", () => {
        resolve(rowCnt);
      });
  });
}

(async () => {
  const folderPath = "/home/sukhi/Downloads/TAM-SAM/tables/";
  const files = getFiles(folderPath);

  var totoalFiles = files.length,
    countProcess = 0;

  if (files.length == 0) {
    console.log("No CSV File found in Folder " + folderPath);
    return;
  }

  fs.mkdirSync(
    path.join(folderPath, "/processFiles"),
    { recursive: true },
    (err) => {
      if (err) throw err;
    }
  );

  for (let x in files) {
    let file = files[x];
    countProcess++;
    console.log("\nFile:- ", file.fileName + file.ext);
    console.log(
      "\t-Started: " + " Total of " + countProcess + " / " + totoalFiles
    );
    let processStats = await processFile(file).catch((err) => {
      console.log(err);
    });

    console.log(`\t-Total Row Processed - ${processStats}`);
    // delete processStats;
  }
  console.log("\nAll Files Have Been Processed");
})();
