/*
## Summary
Extracts raw data from the COKI database (full set). This is not currently meant to be publicly available.

## Description
Downloads a set of metadata for scientific publications, indexed by DOI, from the COKI dataset. To 
run this ETL script, you will need access credentials to your own BigQuery instance and to the COKI 
BigQuery instance.

## Contacts
coki@curtin.edu.au (for enquiries regarding data access)
julian.tonti-filippini@curtin.edu.au (code author)

## License
Apache 2.0
*/
const Telescope = require("./telescope");
const state = {
  name: "raw_papers",
  schema: {
    description: "Extract of publication metadata from the COKI database",
    fields: [
      {
        name: "doi",
        type: "STRING",
        mode: "REQUIRED",
        description: "Digital Object Identifier for the publication",
      },
      {
        name: "year",
        type: "INTEGER",
        mode: "REQUIRED",
        description: "Year of publication",
      },
      {
        name: "cits",
        type: "INTEGER",
        mode: "REQUIRED",
        description: "Number of accumulated citations to date",
      },
      {
        name: "is_oa",
        type: "BOOLEAN",
        mode: "REQUIRED",
        description: "True if the publication is recorded as Open Access",
      },
      {
        name: "type",
        type: "STRING",
        mode: "REQUIRED",
        description:
          "The type of the publication, currently only contains journal-articles",
      },
      {
        name: "issns",
        type: "STRING",
        mode: "REPEATED",
        description:
          "List of ISSNs for the journal associated with this publication",
      },
      {
        name: "rors",
        type: "STRING",
        mode: "REPEATED",
        description:
          "List of ROR codes for institutions affiliated with this publication",
      },
    ],
  },
};

async function run_telescope(conf_user = {}) {
  const args = Object.assign({}, state, conf_user);
  const scope = new Telescope(args);
  scope.log(`Starting telescope: ${__filename}`);
  scope.download(
    `https://storage.googleapis.com/${conf_user.project}-ries/data/papers/2016/000000000000.jsonl.gz`,
    "000.jsonl.gz",
  );
  scope.download(
    `https://storage.googleapis.com/${conf_user.project}-ries/data/papers/2016/000000000001.jsonl.gz`,
    "001.jsonl.gz",
  );
  scope.download(
    `https://storage.googleapis.com/${conf_user.project}-ries/data/papers/2016/000000000002.jsonl.gz`,
    "002.jsonl.gz",
  );
  scope.upload();
  scope.log("Finished");
}

if (require.main === module) {
  config = require("../app").conf();
  console.log("Running with config:");
  console.log(config);
  run_telescope(config);
}
module.exports = run_telescope;
