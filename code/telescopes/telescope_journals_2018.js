/*
## Summary
ETL for the ERA 2018 journal list (and FoRs that come with the list)

## Description
see: https://www.arc.gov.au/evaluating-research/excellence-research-australia/past-era-evaluation

This ETL script downloads the official ERA 2018 journal list (excel file), extracts data into JSONL then uploads to BigQuery

## Contacts
julian.tonti-filippini@curtin.edu.au

## License
Apache 2.0

## Requires
file other.cloud

## Creates
table raw_journals_2018
*/
const Telescope = require("./telescope");
const exjs = require("exceljs");
const state = {
  name: "raw_journals_2018",
  schema: {
    description: "Official ERA 2018 Journal List",
    fields: [
      {
        name: "era_id",
        type: "STRING",
        mode: "REQUIRED",
        description: "A unique identifier assigned by the ARC",
      },
      {
        name: "title",
        type: "STRING",
        mode: "REQUIRED",
        description: "Journal title (English)",
      },
      {
        name: "foreign_title",
        type: "STRING",
        mode: "REQUIRED",
        description: "Journal title (non-English) for foreign journals",
      },
      {
        name: "issns",
        type: "STRING",
        mode: "REPEATED",
        description: "List of ISSNs for the journal",
      },
      {
        name: "forcodes",
        type: "RECORD",
        mode: "REPEATED",
        description:
          "Field of Research (FoR) codes assigned by ARC to the journal.",
        fields: [
          {
            name: "code",
            type: "STRING",
            mode: "REQUIRED",
            description:
              'FoR code (a string because the values are zero padded and can be "MD")',
          },
          {
            name: "name",
            type: "STRING",
            mode: "REQUIRED",
            description: "FoR name",
          },
        ],
      },
      {
        name: "weighted",
        type: "RECORD",
        mode: "REPEATED",
        description:
          "Field of Research (FoR) codes assigned by ARC to the journal.",
        fields: [
          {
            name: "code",
            type: "STRING",
            mode: "REQUIRED",
            description:
              'FoR code (a string because the values are zero padded and can be "MD")',
          },
          {
            name: "weight",
            type: "INTEGER",
            mode: "REQUIRED",
            description:
              "Portional assignment of this FoR code (should sum to 100 for the 2-digit codes and 100 for the 4-digit codes)",
          },
        ],
      },
    ],
  },
};

// transform raw JSON data from zenodo into a reduced jsonl file for upload
async function transform(scope, ifile = "", ofile = "") {
  const FIELD_NAMES = [
    "ERA Journal Id",
    "Title",
    "Foreign Title",
    "FoR 1",
    "FoR 1 Name",
    "FoR 2",
    "FoR 2 Name",
    "FoR 3",
    "FoR 3 Name",
    "ISSN 1",
    "ISSN 2",
    "ISSN 3",
    "ISSN 4",
    "ISSN 5",
    "ISSN 6",
    "ISSN 7",
  ];
  const distinct = (arr = []) => Array.from(new Set(arr)).filter((v) => v);
  const workbook = new exjs.Workbook();
  await workbook.xlsx.readFile(scope.path_local(ifile));
  const journals = [];
  workbook.worksheets[1].eachRow((row, num) => {
    let vals = row.values.slice(1);
    if (num == 1) {
      console.assert(FIELD_NAMES.join("") == vals.join(""));
      return;
    }
    let r = {};
    for (let i = 0; i < FIELD_NAMES.length; ++i) {
      r[FIELD_NAMES[i]] = ((vals[i] ?? "") + "").trim();
    }
    journals.push({
      era_id: r["ERA Journal Id"],
      title: r["Title"],
      foreign_title: r["Foreign Title"],
      issns: distinct([
        r["ISSN 1"],
        r["ISSN 2"],
        r["ISSN 3"],
        r["ISSN 4"],
        r["ISSN 5"],
        r["ISSN 6"],
        r["ISSN 7"],
      ]),
      forcodes: distinct(
        [
          { code: r["FoR 1"] || "MD", name: r["FoR 1 Name"] },
          { code: r["FoR 2"] || "MD", name: r["FoR 2 Name"] },
          { code: r["FoR 3"] || "MD", name: r["FoR 3 Name"] },
        ]
          .filter((v) => v.name)
          .map((v) => JSON.stringify(v)),
      ).map((v) => JSON.parse(v)),
    });
  });

  // deal with FoR codes, splitting into 2s and 4s and assigning weights. Names are added in BigQuery
  journals.forEach((j) => {
    let counts = {};
    let num_2s = 0;
    let num_4s = 0;

    j.forcodes.forEach((fc) => {
      let s4 = fc.code.substring(0, 4);
      let s2 = fc.code.substring(0, 2);
      if (s4.length == 4) {
        num_4s++;
        counts[s4] = (counts[s4] ?? 0) + 1;
      }
      if (s2.length == 2) {
        num_2s++;
        counts[s2] = (counts[s2] ?? 0) + 1;
      }
    });
    j.weighted = [];
    for (let [code, count] of Object.entries(counts)) {
      j.weighted.push({
        code,
        weight: Math.round(
          (count * 100) / (code.length == 4 ? num_4s : num_2s),
        ),
      });
    }
  });
  scope.save(journals, ofile);
}

async function run_telescope(conf_user = {}) {
  const scope = new Telescope(Object.assign(state, conf_user));
  scope.log(`Telescope starting: ${__filename}`);
  await scope.download(
    "https://web.archive.org.au/awa/20220309172834mp_/https://www.arc.gov.au/file/10549/download?token=Sbfb2a9n",
    "era_2018_journal_list.xlsx",
  );
  await scope.transform(transform, "era_2018_journal_list.xlsx", "data.jsonl");
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
