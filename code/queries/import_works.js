/*
2024-Jun-6

A client can provide a list of works with manually assigned FoR codes and weightings. These should
have been processed and uploaded into a GCS bucket (see ../generate.js). This query loads the data
into a new table.

*/
const compile = ({ project = "", dataset = "", version = "" }) => `
-- generated by: ${require("path").basename(__filename)}
BEGIN
  LOAD DATA OVERWRITE \`${project}.${dataset}.raw_forcodes_2008_${version}\` (
    vers STRING OPTIONS(description='ANZSIC version, either 2008 or 2020'),
    code STRING OPTIONS(description='Field of research code (either 2-digit, 4-digit or 6-digit)'),
    name STRING OPTIONS(description='Field of research'),
  )             OPTIONS(description='Official Field of Research (FoR) codes sourced from Official Research Organisation Registry data sourced from http://aria.stats.govt.nz/ and https://www.abs.gov.au/')
  FROM FILES (
    format = 'JSON',
    uris = [
      '${project}-ries/raw_forcodes_2008/data*.jsonl'
    ]
  );
  LOAD DATA OVERWRITE \`${project}.${dataset}.raw_forcodes_2020_${version}\` (
    vers STRING OPTIONS(description='ANZSIC version, either 2008 or 2020'),
    code STRING OPTIONS(description='Field of research code (either 2-digit, 4-digit or 6-digit)'),
    name STRING OPTIONS(description='Field of research'),
  )             OPTIONS(description='Official Field of Research (FoR) codes sourced from Official Research Organisation Registry data sourced from http://aria.stats.govt.nz/ and https://www.abs.gov.au/')
  FROM FILES (
    format = 'JSON',
    uris = [
      '${project}-ries/raw_forcodes_2020/data*.jsonl'
    ]
  );
  CREATE OR REPLACE TABLE \`${project}.${dataset}.raw_forcodes${version}\` AS (
    SELECT '2008' AS vers, code, name FROM \`${project}.${dataset}.raw_forcodes_2008_${version}\` UNION ALL
    SELECT '2020' AS vers, code, name FROM \`${project}.${dataset}.raw_forcodes_2020_${version}\`
  );
END;
`;
const compile_all = (args = {}) => [compile(args)];
module.exports = { compile, compile_all };
if (require.main === module) require("app").cli_compile(compile_all);
