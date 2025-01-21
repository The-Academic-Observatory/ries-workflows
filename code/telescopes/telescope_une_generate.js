/* 
2024-Jun-6

Parse UNE-provided outputs and patch them into a RIES run
*/

const assert = require("assert");
const exjs = require("exceljs");
const exec = require("node:child_process").execSync;
const fs = require("fs");

// determine if an object exists in the local (or cloud) filesystem
function exists(address = "") {
  if (address.startsWith("gs://")) {
    try {
      return address === exec(`gcloud storage ls ${address}`).toString().trim();
    } catch (e) {
      return false;
    }
  }
  return fs.existsSync(address);
}
function insist(address) {
  if (!exists(address)) {
    console.error(`ERROR - no object found at this address: ${address}`);
    process.exit();
  }
}

// load data from an excel file
async function excel_load(ifile) {
  // returns a 3d array[workbook[row[value]]]
  // row[0] has field names / headers
  // each table's rows are clipped and padded to match the size of row[0]
  // missing values are set to null */
  const exceljs = new exjs.Workbook();
  const workbook = await exceljs.xlsx.readFile(ifile);
  const sheets = [];
  workbook.eachSheet((sheet) => {
    const table = [];
    sheets.push(table);
    sheet.eachRow({ includeEmpty: false }, (row_obj) => {
      const values = [];
      row_obj.eachCell({ includeEmpty: true }, (cell_obj) =>
        values.push(cell_obj.value),
      );
      table.push(values);
    });
    table.forEach(
      (row, i) => (table[i] = table[0].map((col, j) => row[j] ?? null)),
    );
  });
  return sheets;
}

// return an object from an array of keys and an array of vals
function zip(keys = [], vals = []) {
  return Object.fromEntries(keys.map((key, i) => [key, vals[i] ?? null]));
}

// sanitise the values in a table against a schema
function table_sanitise(schema = [], table = []) {
  const keys_old = schema.map((v) => v.src);
  const keys_new = schema.map((v) => v.dst);
  const header = table[0];
  keys_old.forEach((key, i) => assert(key === header[i]));
  return table.slice(1).map((row, i) => {
    const sanitised = row.map((val, j) => {
      return val === null || val === undefined
        ? schema[j].default
        : schema[j].type(String(val).trim());
    });
    return zip(keys_new, sanitised);
  });
}

async function run_all({ project = "" }) {
  const cloud_excel = "gs://une_outputs/une_outputs_20240611.xlsx";
  const local_excel = ".data/une_outputs_20240611.xlsx";
  const local_jsonl = ".data/une_outputs_20240611.jsonl";
  const cloud_jsonl = `gs://${project}-ries/data/tables/temp/une_outputs_20240611.jsonl`;
  const schema = [
    { src: "UUID", dst: "uuid", type: String, default: "" },
    { src: "Type", dst: "output_type", type: String, default: "" },
    { src: "DOI", dst: "doi", type: String, default: "" },
    { src: "Link", dst: "url", type: String, default: "" },
    { src: "Journal ISSN", dst: "journal_issn", type: String, default: "" },
    { src: "Journal", dst: "journal_title", type: String, default: "" },
    { src: "Count FoR", dst: "for_count", type: Number, default: 0 },
    { src: "FoR1", dst: "for1_code", type: String, default: "" },
    { src: "FoR1%", dst: "for1_pct", type: Number, default: 0 },
    { src: "FoR2", dst: "for2_code", type: String, default: "" },
    { src: "FoR2%", dst: "for2_pct", type: Number, default: 0 },
    { src: "FoR3", dst: "for3_code", type: String, default: "" },
    { src: "FoR3%", dst: "for3_pct", type: Number, default: 0 },
  ];

  // download
  if (!exists(local_excel)) {
    if (!exists(".data")) {
      exec(`mkdir -p .data`);
    }
    insist(".data");
    insist(cloud_excel);
    exec(`gcloud storage cp ${cloud_excel} ${local_excel}`);
    insist(local_excel);
  }

  // transform
  if (!exists(local_jsonl)) {
    insist(local_excel);
    const table_excel = (await excel_load(local_excel))[0];
    const table_clean = table_sanitise(schema, table_excel);
    table_clean.forEach((v) => {
      if (v.doi.length < 10) {
        v.doi = "";
      }
      v.doi = v.doi.toUpperCase();
      v.for1_pct *= 100;
      v.for2_pct *= 100;
      v.for3_pct *= 100;
      assert(v.for1_pct + v.for2_pct + v.for3_pct == 100);
    });
    console.log("first record", table_clean.at(0));
    console.log("last record", table_clean.at(-1));
    console.log("total records", table_clean.length);

    fs.writeFileSync(
      local_jsonl,
      table_clean.map((v) => JSON.stringify(v)).join("\n"),
    );
    insist(local_jsonl);
  }

  // upload
  if (!exists(cloud_jsonl)) {
    insist(local_jsonl);
    exec(`gcloud storage cp ${local_jsonl} ${cloud_jsonl}`);
    insist(cloud_jsonl);
  }
}

if (require.main === module) {
  const conf = require("../app").conf();
  run_all(conf);
}
module.exports = run_all;
