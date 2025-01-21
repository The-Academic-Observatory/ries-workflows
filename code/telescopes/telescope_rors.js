/*
## Summary
ETL RoR codes from ror.org into BigQuery

## Description
Downloads the most recent data dump from the Research Organization Registry 
Community: https://ror.readme.io/docs/data-dump

Transforms and uploads into a table in BigQuery

## Contacts
julian.tonti-filippini@curtin.edu.au

## License
Apache 2.0
*/
const app = require("../app");
const { execSync } = require("node:child_process");
const Telescope = require("./telescope");
const state = {
  name: "raw_rors",
  schema: {
    description:
      "Official Research Organisation Registry data sourced from https://ror.readme.io/docs/data-dump",
    fields: [
      {
        name: "ror",
        type: "STRING",
        description: "Research Organisation Registry unique identifier",
      },
      {
        name: "since",
        type: "STRING",
        description: "The year that the institution was established",
      },
      {
        name: "status",
        type: "STRING",
        description: "Current status of the institution (filter for active)",
      },
      {
        name: "type_0",
        type: "STRING",
        description: "Primary activity type (ie, types[0])",
      },
      {
        name: "country",
        type: "STRING",
        description: "Country in which the research institution is located",
      },
      {
        name: "name",
        type: "STRING",
        description: "Name of the research institution",
      },
      {
        name: "link_0",
        type: "STRING",
        description: "Primary URL (ie, links[0])",
      },
      {
        name: "types",
        type: "STRING",
        mode: "REPEATED",
        description: "All activity types that the institution is engaged in",
      },
      {
        name: "links",
        type: "STRING",
        mode: "REPEATED",
        description: "All links associated with the institution",
      },
    ],
  },
};

// expand the zipfile from zenodo and map it into a new JSONL file
async function transform(scope, ifile, ofile) {
  execSync(`
    cd ${scope.path_local()};
    unzip ${ifile};
    mv *ror-data.json zenodo_data.json;
    mv *ror-data.csv  zenodo_data.csv;
  `);
  scope.save(
    scope.load("zenodo_data.json").map((v) => ({
      ror: v.id,
      since: v.established + "",
      status: v.status,
      type_0: v.types[0],
      country: v.country.country_name,
      name: v.name,
      link_0: v.links[0],
      types: v.types,
      links: v.links,
    })),
    ofile,
  );
}

async function run_telescope(conf_user = {}) {
  const scope = new Telescope(Object.assign(state, conf_user));
  scope.log(`Telescope starting: ${__filename}`);
  await scope.download(
    "https://zenodo.org/api/records/?communities=ror-data",
    "zenodo_meta.json",
  );
  const link =
    scope.load("zenodo_meta.json")?.hits?.hits[0]?.files?.pop().links?.self ??
    "";
  await scope.download(link, "zenodo_data.zip");
  await scope.transform(transform, "zenodo_data.zip", "data.jsonl");
  await scope.upload();
  scope.log("Telescope finished");
}

if (require.main === module) {
  const config = app.conf();
  console.log("Running with config:");
  console.log(config);
  run_telescope(config);
}
module.exports = run_telescope;
