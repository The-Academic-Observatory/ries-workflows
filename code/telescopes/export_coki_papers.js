/*
## Summary
Export a set of journal-articles from COKI, filtered according to ERA parameters.

## Description
The RIES workflow imports a set of raw data tables from various external sources. For consistency,
this script will treat the COKI DOI table as one of those third party sources. It exports the 
desired papers to a GCloud bucket as a series of gzipped CSV files. These are then re-imported to 
RIES via a telescope.

A full set of works is exported into the RIES private bucket. A restricted set (for a single year)
is exported into the RIES public bucket.

## Contacts
julian.tonti-filippini@curtin.edu.au

## License
Apache 2.0
*/
async function main() {
  const args = require("../app").conf();
  const coki = require("./doi_latest.js");
  const sql = require("../queries/export_raw_papers.js");
  const { run_query } = require("../utilities/run_queries.js");

  if (args.coki_table == "" || args.coki_table == "latest") {
    args.coki_table = await coki.latest();
  }

  const query_out = run_query(args.project, sql.compile(args));
  if (query_out.success == false) {
    console.log(`Error: ${query_out.message}`);
  } else {
    console.log("Success!");
  }
}

if (require.main === module) {
  main();
}
