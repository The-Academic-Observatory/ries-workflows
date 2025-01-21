/*
## Summary
Minimal ETL for ISSN data

## Description
Downloads the authoritative ISSN <-> ISSN-L mapping from issn.org then uploads it into BigQuery.

## Contacts
julian.tonti-filippini@curtin.edu.au

## License
Apache 2.0

## Requires
file other.cloud

## Creates
table raw_issns
*/
const execSync = require("child_process").execSync;
const Telescope = require("./telescope");
const state = {
  name: "raw_issns",
  schema: {
    description:
      "Official ISSN -> ISSN-L mapping from https://www.issn.org/wp-content/uploads/2014/03/issnltables.zip",
    fields: [
      { name: "issn", type: "STRING", mode: "REQUIRED", description: "ISSN" },
      {
        name: "issnl",
        type: "STRING",
        mode: "REQUIRED",
        description: "Linking ISSN",
      },
    ],
  },
};

// transform to JSONL file for upload to bigquery
async function transform(scope, ifile, ofile) {
  const tfile = "data.lines";
  execSync(`
    cd ${scope.path_local()};
    unzip ${ifile};
    mv *.ISSN-to-ISSN-L.txt ${tfile};
  `);
  const rx = /^[0-9]{4}-[0-9]{3}[0-9X]$/;
  const ok = (v) => {
    v = v.trim().toUpperCase();
    return rx.test(v) ? v : "";
  };
  scope.save(
    scope
      .load(tfile)
      .slice(1)
      .map((v) => {
        let [issn, issnl, code] = v.split("\t");
        issn = ok(issn);
        issnl = ok(issnl);
        return issn && issnl ? { issn, issnl } : null;
      })
      .filter((v) => v),
    ofile,
  );
}

async function run_telescope(conf_user = {}) {
  const scope = new Telescope(Object.assign(state, conf_user));
  scope.log(`Telescope starting: ${__filename}`);
  await scope.download(
    "https://www.issn.org/wp-content/uploads/2014/03/issnltables.zip",
    "data.zip",
  );
  await scope.transform(transform, "data.zip", "data.jsonl");
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
