/*
## Summary
Extract publication metrics from COKI's database. Creates both a public (limited) and private (unlimited) dataset.

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
const { run_query } = require("../utilities/run_queries.js");
const compile_query = require("../queries/export_raw_papers").compile;
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

async function run_telescope(conf = {}) {
  const scope = new Telescope(Object.assign(state, conf));
  scope.log(`Starting telescope: ${__filename}`);

  const query = compile_query({
    coki_project: conf.coki_project,
    coki_dataset: conf.coki_dataset,
    doi_table_version: conf.doi_table_version,
    project: conf.project,
    start: conf.start,
    finish: conf.finish,
  });
  run_query(conf.project, query);
  scope.log("Finished!");
}

if (require.main === module) {
  const config = require("../app").conf();
  console.log("Running with config:");
  console.log(config);
  run_telescope(config);
}
module.exports = run_telescope;
