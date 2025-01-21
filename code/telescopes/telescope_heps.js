/*
## Summary
ETL HEPs from a static list into BigQuery

## Description
The ERA documentation defines a subset of Australian Higher Education Providers (HEPs) that are specifically analysed in the ERA process. The list can be found in the /docs directory. This telescope simply uploads it into BigQuery as a raw list of RoR codes which will then be processed by SQL query for a little more processing.

## Contacts
julian.tonti-filippini@curtin.edu.au

## License
Apache 2.0

## Requires
file other.cloud

## Creates
table raw_heps
*/
const Telescope = require("./telescope");
const state = {
  name: "raw_heps",
  schema: {
    description:
      "List of Australian Higher Education Providers (HEPs) focused on by the ERA process",
    fields: [
      {
        name: "ror",
        type: "STRING",
        description: "Research Organisation Registry unique identifier",
      },
      {
        name: "name",
        type: "STRING",
        description: "Name of the research institution",
      },
    ],
  },
};
const data = [
  // TODO: find an online source for this information. These were populated manually from the RoR website
  { ror: "https://ror.org/04cxm4j25", name: "Australian Catholic University" },
  {
    ror: "https://ror.org/019wvm592",
    name: "The Australian National University",
  },
  {
    ror: "https://ror.org/03n0gvg35",
    name: "Batchelor Institute of Indigenous Tertiary Education",
  },
  { ror: "https://ror.org/006jxzx88", name: "Bond University" },
  { ror: "https://ror.org/023q4bk22", name: "Central Queensland University" },
  { ror: "https://ror.org/048zcaj52", name: "Charles Darwin University" },
  { ror: "https://ror.org/00wfvh315", name: "Charles Sturt University" },
  { ror: "https://ror.org/02n415q13", name: "Curtin University" },
  { ror: "https://ror.org/02czsnj07", name: "Deakin University" },
  { ror: "https://ror.org/05jhnwe22", name: "Edith Cowan University" },
  { ror: "https://ror.org/05qbzwv83", name: "Federation University" },
  { ror: "https://ror.org/01kpzv902", name: "Flinders University" },
  { ror: "https://ror.org/02sc3r913", name: "Griffith University" },
  { ror: "https://ror.org/04gsp2c11", name: "James Cook University" },
  { ror: "https://ror.org/01rxfrp27", name: "La Trobe University" },
  { ror: "https://ror.org/01sf06y89", name: "Macquarie University" },
  { ror: "https://ror.org/02bfwt286", name: "Monash University" },
  { ror: "https://ror.org/00r4sry34", name: "Murdoch University" },
  {
    ror: "https://ror.org/03pnv4752",
    name: "Queensland University of Technology",
  },
  {
    ror: "https://ror.org/04ttjf776",
    name: "Royal Melbourne Institute of Technology",
  },
  { ror: "https://ror.org/001xkv632", name: "Southern Cross University" },
  {
    ror: "https://ror.org/031rekg67",
    name: "Swinburne University of Technology",
  },
  { ror: "https://ror.org/0351xae06", name: "Torrens University Australia" },
  { ror: "https://ror.org/03r8z3t63", name: "University of New South Wales" },
  { ror: "https://ror.org/00892tw58", name: "University of Adelaide" },
  { ror: "https://ror.org/04s1nv328", name: "University of Canberra" },
  { ror: "https://ror.org/02xn8bh65", name: "University of Divinity" },
  { ror: "https://ror.org/01ej9dk98", name: "University of Melbourne" },
  { ror: "https://ror.org/04r659a56", name: "University of New England" },
  { ror: "https://ror.org/00eae9z71", name: "University of Newcastle" },
  {
    ror: "https://ror.org/02stey378",
    name: "University of Notre Dame Australia",
  },
  { ror: "https://ror.org/00rqy9422", name: "University of Queensland" },
  { ror: "https://ror.org/01p93h210", name: "University of South Australia" },
  {
    ror: "https://ror.org/04sjbnx57",
    name: "University of Southern Queensland",
  },
  { ror: "https://ror.org/0384j8v12", name: "University of Sydney" },
  { ror: "https://ror.org/01nfmeh72", name: "University of Tasmania" },
  {
    ror: "https://ror.org/03f0f6041",
    name: "University of Technology, Sydney",
  },
  { ror: "https://ror.org/047272k79", name: "University of Western Australia" },
  { ror: "https://ror.org/00jtmb277", name: "University of Wollongong" },
  {
    ror: "https://ror.org/016gb9e15",
    name: "University of the Sunshine Coast",
  },
  { ror: "https://ror.org/04j757h98", name: "Victoria University" },
  { ror: "https://ror.org/03t52dk35", name: "Western Sydney University" },
];
async function run_telescope(conf_user = {}) {
  const scope = new Telescope(Object.assign(state, conf_user));
  scope.log(`Telescope starting: ${__filename}`);
  scope.save(data, "data.jsonl");
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
