/*
## Summary

ETL for Field of Research codes (ANZSIC 2008)

## Description

Codes sourced from: https://www.abs.gov.au/statistics/classifications/australian-and-new-zealand-standard-research-classification-anzsrc/latest-release
Transformed and uploaded into BigQuery

## Contacts
julian.tonti-filippini@curtin.edu.au

## License
Apache 2.0

-- ## Requires
file other.cloud

-- ## Creates
table raw_forcodes_2008
*/
const app = require("../app");
const Telescope = require("./telescope");
const state = {
  name: "raw_forcodes_2008",
  schema: {
    description:
      "Official Field of Research (FoR) codes sourced from http://aria.stats.govt.nz/ and https://www.abs.gov.au/",
    fields: [
      {
        name: "vers",
        type: "STRING",
        description: "ANZSIC version, either 2008 or 2020",
      },
      {
        name: "code",
        type: "STRING",
        description:
          "Field of research code (either 2-digit, 4-digit or 6-digit)",
      },
      { name: "name", type: "STRING", description: "Field of research" },
    ],
  },
};

// transforms 2020 FoR codes from a CSV dump (the HTML method can't be used because special characters aren't properly encoded by the Aria site)
async function transform(scope, ifile, ofile) {
  const codes = {};
  const lines = scope
    .load(ifile)
    .split("Code,Descriptor,")[1]
    .trim()
    .split("\n");
  for (let line of lines) {
    let [code, ...name] = line.split(",");
    code = code.trim();
    name = name
      .filter((s) => s)
      .join(",")
      .split('"')
      .join("");
    //codes[code] = name;
    codes[code] = name.replaceAll("’", "'");
  }
  const records = [];
  for (let key of Object.keys(codes).sort()) {
    records.push({
      vers: "2008",
      code: key,
      name: codes[key],
    });
  }
  scope.save(records, ofile);
  app.save(records, "docs/for_codes_2008.jsonlr");
}

async function run_telescope(conf_user = {}) {
  const scope = new Telescope(Object.assign(state, conf_user));
  scope.log(`Telescope starting: ${__filename}`);
  await scope.download(
    "https://aria.stats.govt.nz/aria/Aria/Download?filename=1E070FFAD15758035BCF8F3EF0DDBB4EF1452DF1\\\\7\\\\7FD15C21DEE2899C553877FC3150BC3B55B687D1.csv&request=FxKWozCj",
    "data.txt",
  );
  await scope.transform(transform, "data.txt", "data.jsonl");
  await scope.upload();
  scope.log("Finished");
}

if (require.main === module) {
  config = app.conf();
  console.log("Running with config:");
  console.log(config);
  run_telescope(config);
}
module.exports = run_telescope;
