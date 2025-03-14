/*
## Summary
ETL for the ERA 2018 journal list (and FoRs that come with the list)

## Description

## Contacts
julian.tonti-filippini@curtin.edu.au

## License
Apache 2.0

## Requires
file other.cloud

## Creates
table era_historical_ratings
*/
const Telescope = require("./telescope");
const state = {
  name: "raw_history",
  schema: {
    description:
      "Official ERA outcomes table from: https://dataportal.arc.gov.au/ERA/Web/Outcomes#/institution/",
    fields: [
      {
        name: "hep_code",
        type: "STRING",
        mode: "REQUIRED",
        description:
          "short-code for the Australian institution (higher education provider)",
      },
      {
        name: "hep_name",
        type: "STRING",
        mode: "REQUIRED",
        description: "institution name",
      },
      {
        name: "for_vers",
        type: "STRING",
        mode: "REQUIRED",
        description: "version of the field of research codes being used",
      },
      {
        name: "for_code",
        type: "STRING",
        mode: "REQUIRED",
        description: "field of research code",
      },
      {
        name: "for_name",
        type: "STRING",
        mode: "REQUIRED",
        description: "field of research name",
      },
      {
        name: "era_2010",
        type: "STRING",
        mode: "REQUIRED",
        description: "ERA rating assigned in 2010 (NA = not assessed)",
      },
      {
        name: "era_2012",
        type: "STRING",
        mode: "REQUIRED",
        description: "ERA rating assigned in 2012 (NA = not assessed)",
      },
      {
        name: "era_2015",
        type: "STRING",
        mode: "REQUIRED",
        description: "ERA rating assigned in 2015 (NA = not assessed)",
      },
      {
        name: "era_2018",
        type: "STRING",
        mode: "REQUIRED",
        description: "ERA rating assigned in 2018 (NA = not assessed)",
      },
    ],
  },
};
const hep_codes = [
  "ACU",
  "ADE",
  "ANU",
  "BAT",
  "BON",
  "CAN",
  "CDU",
  "CQU",
  "CSU",
  "CUT",
  "DIV",
  "DKN",
  "ECU",
  "FED",
  "FLN",
  "GRF",
  "JCU",
  "LTU",
  "MEL",
  "MON",
  "MQU",
  "MUR",
  "NDA",
  "NEW",
  "NSW",
  "QLD",
  "QUT",
  "RMT",
  "SCU",
  "SWN",
  "SYD",
  "TAS",
  "TOR",
  "UNE",
  "USA",
  "USC",
  "USQ",
  "UTS",
  "UWA",
  "VIC",
  "WOL",
  "WSU",
];

// transform raw JSON data from zenodo into a reduced jsonl file for upload
async function transform(scope, ifile, ofile) {
  let table = [];
  for (let code of hep_codes) {
    scope.load(`${code}.json`).data.forEach((rec) => {
      table.push({
        hep_code: rec.attributes.institution["short-name"],
        hep_name: rec.attributes.institution.name,
        for_vers: "2008",
        for_code: rec.attributes["field-of-research"].code,
        for_name: rec.attributes["field-of-research"].name,
        era_2010: rec.attributes.outcomes[0].value ?? "NA",
        era_2012: rec.attributes.outcomes[1].value ?? "NA",
        era_2015: rec.attributes.outcomes[2].value ?? "NA",
        era_2018: rec.attributes.outcomes[3].value ?? "NA",
      });
    });
  }
  scope.save(table, ofile);
}
async function run_telescope(conf_user = {}) {
  const scope = new Telescope(Object.assign(state, conf_user));
  scope.log(`Telescope starting: ${__filename}`);
  for (let code of hep_codes) {
    await scope.download(
      `https://dataportal.arc.gov.au/ERA/API/ratings?page%5Bsize%5D=179&page%5Bnumber%5D=1&filter=${code}`,
      `${code}.json`,
    );
  }
  await scope.transform(transform, "", "data.jsonl");
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
