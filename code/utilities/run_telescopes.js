/*
## Summary
Run ETL scripts to (re)build the raw data files in Cloud Storage.

## Description

## Contacts
julian.tonti-filippini@curtin.edu.au

## License
Apache 2.0
*/
const app = require("app");

async function run_all(conf) {
  //await app.query(`CREATE SCHEMA IF NOT EXISTS \`${conf.ns_core}\``);
  //await require('../loaders/telescope_coki')         (conf);
  await require("../loaders/telescope_forcodes_2008")(conf);
  await require("../loaders/telescope_forcodes_2020")(conf);
  //await require('../loaders/telescope_forcodes')     (conf);
  await require("../loaders/telescope_heps")(conf);
  await require("../loaders/telescope_history")(conf);
  await require("../loaders/telescope_issns")(conf);
  await require("../loaders/telescope_journals_2018")(conf);
  await require("../loaders/telescope_journals_2023")(conf);
  await require("../loaders/telescope_rors")(conf);
}
module.exports = run_all;

if (require.main === module) module.exports(app.conf());
