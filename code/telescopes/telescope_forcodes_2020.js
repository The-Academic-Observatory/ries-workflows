/*
## Summary

ETL for Field of Research codes (ANZSIC 2020)

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
table raw_forcodes_2020
*/
const app = require("../app");
const Telescope = require("./telescope");
const state = {
  name: "raw_forcodes_2020",
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
      vers: "2020",
      code: key,
      name: codes[key],
    });
  }
  scope.save(records, ofile);
  app.save(records, "docs/for_codes_2020.jsonlr");
}

async function run_telescope(conf_user = {}) {
  const scope = new Telescope(Object.assign(state, conf_user));
  scope.log(`Telescope starting: ${__filename}`);
  await scope.download(
    "https://aria.stats.govt.nz/aria/Aria/Download?filename=D06E4D1F26C55127D3203BC02046628214D1B15F\\19\\C82F16F639DA44EA9EA2A3AF2B0A681C94B440D2.csv&request=PuCMXDCt",
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
