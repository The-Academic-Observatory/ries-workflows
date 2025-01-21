/*
## Summary

ETL for Field of Education codes (ASCED 2001)

## Description

Codes sourced from: https://www.abs.gov.au/statistics/classifications/australian-standard-classification-education-asced/latest-release

Transformed and uploaded into BigQuery

## Contacts
julian.tonti-filippini@curtin.edu.au

## License
Apache 2.0

-- ## Requires
file other.cloud

-- ## Creates
table raw_foecodes
*/
const Telescope = require("./telescope");
const exjs = require("exceljs");
const state = {
  name: "raw_foecodes",
  schema: {
    description:
      "Official Field of Education (FoE) codes sourced from https://www.abs.gov.au/",
    fields: [
      { name: "vers", type: "STRING", description: "ASCED version" },
      {
        name: "code",
        type: "STRING",
        description:
          "Field of education code (either 2-digit, 4-digit or 6-digit)",
      },
      { name: "name", type: "STRING", description: "Field of education" },
    ],
  },
};

// transform raw JSON data from zenodo into a reduced jsonl file for upload
async function transform(scope, ifile = "", ofile = "") {
  const workbook = new exjs.Workbook();
  await workbook.xlsx.readFile(scope.path_local(ifile));
  const rows = [];
  const vers = "2001";
  workbook.worksheets[2].eachRow((row, num) => {
    let [code, name] = row.values.filter((v) => v);
    if (!name) return;
    name = name.replace(", n.e.c.", "").trim();
    rows.push({ vers, code, name });
  });
  scope.save(rows, ofile);
}

async function run_telescope(conf_user = {}) {
  const scope = new Telescope(Object.assign(state, conf_user));
  scope.log(`Telescope starting: ${__filename}`);
  await scope.download(
    "https://www.abs.gov.au/statistics/classifications/australian-standard-classification-education-asced/2001/1272.0%20australian%20standard%20classification%20of%20education%20%28asced%29%20structures.xlsx",
    "src_foe_codes.xlsx",
  );
  await scope.transform(transform, "src_foe_codes.xlsx", "data.jsonl");
  await scope.upload();
  scope.log("Finished");
}

if (require.main === module) {
  config = require("../app").conf();
  console.log("Running with config:");
  console.log(config);
  run_telescope(config);
}
module.exports = run_telescope;
