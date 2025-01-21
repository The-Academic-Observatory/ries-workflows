/*
## Summary
Telescope manager. All other telecopes can be run from here

## Description
Given a set of dataset names, this manager will attempt to update all matching telescopes.

## Contacts
julian.tonti-filippini@curtin.edu.au

## License
Apache 2.0
*/
const app = require("app");
const scopes = [
  "./telescope_coki",
  "./telescope_forcodes_2008",
  "./telescope_forcodes_2020",
  "./telescope_heps",
  "./telescope_history",
  "./telescope_issns",
  "./telescope_journals_2018",
  "./telescope_journals_2023",
  "./telescope_papers",
  "./telescope_rors",
  "./telescope_une_generate",
]
  .map((file) => {
    try {
      return require(file);
    } catch (e) {
      return null;
    }
  })
  .filter((v) => v);

async function run_all(args = {}) {
  app.log("Running all telescopes");
  for (let scope of scopes) {
    await scope(args);
  }
  app.log("Finished all telescopes");
}
module.exports = { scopes, run_all };

if (require.main === module) {
  run_all(app.conf());
}

