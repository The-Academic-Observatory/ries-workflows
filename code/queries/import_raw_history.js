/*
## Summary
Imports raw data from GCS into the ERA historical ratings table.

## Contacts
julian.tonti-filippini@curtin.edu.au

## License
Apache 2.0

## Requires
file coki.cloud

## Creates
table era_historical_ratings
*/
const compile = ({ project = "", dataset = "", version = "" }) => `
-- generated by: ${require("path").basename(__filename)}
BEGIN
  LOAD DATA OVERWRITE \`${project}.${dataset}.era_historical_ratings${version}\` (
    hep_code STRING NOT NULL OPTIONS(description='short-code for the Australian institution (higher education provider)'),
    hep_name STRING NOT NULL OPTIONS(description='institution name'),
    for_vers STRING NOT NULL OPTIONS(description='version of the field of research codes being used'),
    for_code STRING NOT NULL OPTIONS(description='field of research code'),
    for_name STRING NOT NULL OPTIONS(description='field of research name'),
    era_2010 STRING NOT NULL OPTIONS(description='ERA rating assigned in 2010 (NA = not assessed)'),
    era_2012 STRING NOT NULL OPTIONS(description='ERA rating assigned in 2012 (NA = not assessed)'),
    era_2015 STRING NOT NULL OPTIONS(description='ERA rating assigned in 2015 (NA = not assessed)'),
    era_2018 STRING NOT NULL OPTIONS(description='ERA rating assigned in 2018 (NA = not assessed)'),
  )                          OPTIONS(description='Official ERA outcomes table from: https://dataportal.arc.gov.au/ERA/Web/Outcomes#/institution/')
  FROM FILES (
    format = 'JSON',
    uris = ['gs://${project}-ries/tables/raw_history/data*.jsonl']
  );
END;
`;
const compile_all = (args = {}) => [compile(args)];
module.exports = { compile, compile_all };
if (require.main === module) require("app").cli_compile(compile_all);
