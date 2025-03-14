/*
## Summary
Assigns RCI categories to outputs or groups.

## Description
For methodology, see:
https://github.com/Curtin-Open-Knowledge-Initiative/coki-ries/blob/main/docs/era_2018.md#indicator-distribution-of-papers-by-rci-classes

## Contacts
julian.tonti-filippini@curtin.edu.au

## License
Apache 2.0

## Requires
table core_papers
table rci_papers
table rci_grouping_*
table benchmarks_rci_*

## Creates
table rci_classes_papers
table rci_classes_fields
table rci_classes_summary
*/
// TODO: add grouping support and refactor table names
const compile = ({
  project = "",
  dataset = "",
  scope = "world",
  digits = 4,
  replace = false,
  version = "",
}) => `
-- generated by: ${require("path").basename(__filename)}
BEGIN 
  -- assign RCI classes to the papers
  ${replace ? "CREATE OR REPLACE TABLE" : "CREATE TABLE IF NOT EXISTS"} \`${project}.${dataset}.rci_classes_papers${version}\` AS (
    SELECT 
      doi,
      A.field,
      A.year,

      CASE
        WHEN rci_world > B.d_c4 THEN 5
        WHEN rci_world > B.d_c3 THEN 4
        WHEN rci_world > B.d_c2 THEN 3
        WHEN rci_world > B.d_c1 THEN 2
        WHEN rci_world > B.d_c0 THEN 1
        ELSE 0
      END AS rci_class_dynamic_world,

      CASE
        WHEN rci_world > B.s_c5 THEN 6
        WHEN rci_world > B.s_c4 THEN 5
        WHEN rci_world > B.s_c3 THEN 4
        WHEN rci_world > B.s_c2 THEN 3
        WHEN rci_world > B.s_c1 THEN 2
        WHEN rci_world > B.s_c0 THEN 1
        ELSE 0
      END AS rci_class_static_world,

      CASE
        WHEN rci_local > B.s_c5 THEN 6
        WHEN rci_local > B.s_c4 THEN 5
        WHEN rci_local > B.s_c3 THEN 4
        WHEN rci_local > B.s_c2 THEN 3
        WHEN rci_local > B.s_c1 THEN 2
        WHEN rci_local > B.s_c0 THEN 1
        ELSE 0
      END AS rci_class_static_local,

      CASE
        WHEN hpi_world > B.s_c5 THEN 6
        WHEN hpi_world > B.s_c4 THEN 5
        WHEN hpi_world > B.s_c3 THEN 4
        WHEN hpi_world > B.s_c2 THEN 3
        WHEN hpi_world > B.s_c1 THEN 2
        WHEN hpi_world > B.s_c0 THEN 1
        ELSE 0
      END AS hpi_class_static_world

    FROM \`${project}.${dataset}.rci_papers${version}\` AS A
    LEFT JOIN \`${project}.${dataset}.benchmarks_rci_${scope}_${digits}_${version}\` AS B ON A.field = B.field AND A.year = B.year
    ORDER BY field,year
  );

  -- assign static RCI classes to fields of research
  ${replace ? "CREATE OR REPLACE TABLE" : "CREATE TABLE IF NOT EXISTS"} \`${project}.${dataset}.rci_classes_fields${version}\` AS (
    SELECT
      A.institution,
      A.field,
      A.year,
      CASE
        WHEN rci_world > B.d_c4 THEN 5
        WHEN rci_world > B.d_c3 THEN 4
        WHEN rci_world > B.d_c2 THEN 3
        WHEN rci_world > B.d_c1 THEN 2
        WHEN rci_world > B.d_c0 THEN 1
        ELSE 0
      END AS rci_class_dynamic_world,

      CASE
        WHEN rci_world > B.s_c5 THEN 6
        WHEN rci_world > B.s_c4 THEN 5
        WHEN rci_world > B.s_c3 THEN 4
        WHEN rci_world > B.s_c2 THEN 3
        WHEN rci_world > B.s_c1 THEN 2
        WHEN rci_world > B.s_c0 THEN 1
        ELSE 0
      END AS rci_class_static_world,

      CASE
        WHEN rci_local > B.s_c5 THEN 6
        WHEN rci_local > B.s_c4 THEN 5
        WHEN rci_local > B.s_c3 THEN 4
        WHEN rci_local > B.s_c2 THEN 3
        WHEN rci_local > B.s_c1 THEN 2
        WHEN rci_local > B.s_c0 THEN 1
        ELSE 0
      END AS rci_class_static_local,

      CASE
        WHEN hpi_world > B.s_c5 THEN 6
        WHEN hpi_world > B.s_c4 THEN 5
        WHEN hpi_world > B.s_c3 THEN 4
        WHEN hpi_world > B.s_c2 THEN 3
        WHEN hpi_world > B.s_c1 THEN 2
        WHEN hpi_world > B.s_c0 THEN 1
        ELSE 0
      END AS hpi_class_static_world
    FROM \`${project}.${dataset}.rci_grouping_${scope}_${digits}_institution_field_year${version}\` AS A
    LEFT JOIN \`${project}.${dataset}.benchmarks_rci_${scope}_${digits}_${version}\` AS B ON A.field = B.field AND A.year = B.year
  );

  -- generate a summary table for the static method
  -- TODO: use weighted average apportionment to calculate a single RCI for a paper
  ${replace ? "CREATE OR REPLACE TABLE" : "CREATE TABLE IF NOT EXISTS"} \`${project}.${dataset}.rci_classes_summary${version}\` AS (
    SELECT 
      I AS institution,
      F.code AS field,
      COUNTIF(rci_class_static_world = 0) AS total_0,
      COUNTIF(rci_class_static_world = 1) AS total_1,
      COUNTIF(rci_class_static_world = 2) AS total_2,
      COUNTIF(rci_class_static_world = 3) AS total_3,
      COUNTIF(rci_class_static_world = 4) AS total_4,
      COUNTIF(rci_class_static_world = 5) AS total_5,
      COUNTIF(rci_class_static_world = 6) AS total_6,
      ROUND(SUM(IF(rci_class_static_world  = 0, F.weight, 0)),2) AS portions_0,
      ROUND(SUM(IF(rci_class_static_world  = 1, F.weight, 0)),2) AS portions_1,
      ROUND(SUM(IF(rci_class_static_world  = 2, F.weight, 0)),2) AS portions_2,
      ROUND(SUM(IF(rci_class_static_world  = 3, F.weight, 0)),2) AS portions_3,
      ROUND(SUM(IF(rci_class_static_world  = 4, F.weight, 0)),2) AS portions_4,
      ROUND(SUM(IF(rci_class_static_world  = 5, F.weight, 0)),2) AS portions_5,
      ROUND(SUM(IF(rci_class_static_world  = 6, F.weight, 0)),2) AS portions_6,

      COUNTIF(rci_class_static_world <= 1) AS rci_low , -- tally of papers in classes 0,1 using FoR fractions
      COUNTIF(rci_class_static_world >= 4) AS rci_high, -- tally of papers in classes 4,5,6 using FoR fractions
      0.0                                  AS rci_ratio -- ratio of rci_high to rci_low

      --hep_for_avg  , -- the percentage of papers (from all HEPs) in this class
      --hep_for_pct  , -- the percentage of papers (from the HEP) of all HEPs' papers in this class
    FROM \`${project}.${dataset}.core_papers${version}\` AS A
    INNER JOIN UNNEST(heps) AS I
    INNER JOIN UNNEST(fors) AS F
    LEFT JOIN \`${project}.${dataset}.rci_classes_papers${version}\` AS B ON A.doi = B.doi
    GROUP BY institution,field
    ORDER BY institution,field
  );
  UPDATE \`${project}.${dataset}.rci_classes_summary${version}\` SET rci_ratio = IF(rci_low=0, rci_high, rci_high / rci_low) WHERE true;
END;`;
const compile_all = (args = {}) => [compile(args)];
module.exports = { compile, compile_all };
if (require.main === module) require("app").cli_compile(compile_all);

const compile2 = ({
  project = "",
  dataset = "",
  replace = false,
  version = "",
}) => `
-- generated by: ${require("path").basename(__filename)}
BEGIN 
  -- assign RCI classes to the papers
  ${replace ? "CREATE OR REPLACE TABLE" : "CREATE TABLE IF NOT EXISTS"} \`${project}.${dataset}.rci_classes_papers${version}\` AS (
    SELECT 
      work,
      A.conc,
      A.year,

      CASE
        WHEN rci > B.d_c4 THEN 5
        WHEN rci > B.d_c3 THEN 4
        WHEN rci > B.d_c2 THEN 3
        WHEN rci > B.d_c1 THEN 2
        WHEN rci > B.d_c0 THEN 1
        ELSE 0
      END AS rci_class_dynamic,

      CASE
        WHEN rci > B.s_c5 THEN 6
        WHEN rci > B.s_c4 THEN 5
        WHEN rci > B.s_c3 THEN 4
        WHEN rci > B.s_c2 THEN 3
        WHEN rci > B.s_c1 THEN 2
        WHEN rci > B.s_c0 THEN 1
        ELSE 0
      END AS rci_class_static

      CASE
        WHEN hpi > B.s_c5 THEN 6
        WHEN hpi > B.s_c4 THEN 5
        WHEN hpi > B.s_c3 THEN 4
        WHEN hpi > B.s_c2 THEN 3
        WHEN hpi > B.s_c1 THEN 2
        WHEN hpi > B.s_c0 THEN 1
        ELSE 0
      END AS hpi_class_static

    FROM \`${project}.${dataset}.rci_papers${version}\` AS A
    LEFT JOIN \`${project}.${dataset}.benchmarks_rci${version}\` AS B ON A.conc = B.conc AND A.year = B.year
    ORDER BY conc,year
  );

  -- assign static RCI classes to research concepts
  ${replace ? "CREATE OR REPLACE TABLE" : "CREATE TABLE IF NOT EXISTS"} \`${project}.${dataset}.rci_classes_concs${version}\` AS (
    SELECT
      A.inst,
      A.conc,
      A.year,
      CASE
        WHEN rci > B.d_c4 THEN 5
        WHEN rci > B.d_c3 THEN 4
        WHEN rci > B.d_c2 THEN 3
        WHEN rci > B.d_c1 THEN 2
        WHEN rci > B.d_c0 THEN 1
        ELSE 0
      END AS rci_class_dynamic,

      CASE
        WHEN rci > B.s_c5 THEN 6
        WHEN rci > B.s_c4 THEN 5
        WHEN rci > B.s_c3 THEN 4
        WHEN rci > B.s_c2 THEN 3
        WHEN rci > B.s_c1 THEN 2
        WHEN rci > B.s_c0 THEN 1
        ELSE 0
      END AS rci_class_static,

      CASE
        WHEN hpi > B.s_c5 THEN 6
        WHEN hpi > B.s_c4 THEN 5
        WHEN hpi > B.s_c3 THEN 4
        WHEN hpi > B.s_c2 THEN 3
        WHEN hpi > B.s_c1 THEN 2
        WHEN hpi > B.s_c0 THEN 1
        ELSE 0
      END AS hpi_class_static
    FROM \`${project}.${dataset}.rci_grouping_inst_conc_year${version}\` AS A
    LEFT JOIN \`${project}.${dataset}.benchmarks_rci${version}\` AS B ON A.conc = B.conc AND A.year = B.year
  );

  -- generate a summary table for the static method
  -- TODO: use weighted average apportionment to calculate a single RCI for a paper
  ${replace ? "CREATE OR REPLACE TABLE" : "CREATE TABLE IF NOT EXISTS"} \`${project}.${dataset}.rci_classes_summary${version}\` AS (
    SELECT 
      I AS inst,
      F.code AS conc,
      COUNTIF(rci_class_static = 0) AS total_0,
      COUNTIF(rci_class_static = 1) AS total_1,
      COUNTIF(rci_class_static = 2) AS total_2,
      COUNTIF(rci_class_static = 3) AS total_3,
      COUNTIF(rci_class_static = 4) AS total_4,
      COUNTIF(rci_class_static = 5) AS total_5,
      COUNTIF(rci_class_static = 6) AS total_6,
      ROUND(SUM(IF(rci_class_static  = 0, F.weight, 0)),2) AS portions_0,
      ROUND(SUM(IF(rci_class_static  = 1, F.weight, 0)),2) AS portions_1,
      ROUND(SUM(IF(rci_class_static  = 2, F.weight, 0)),2) AS portions_2,
      ROUND(SUM(IF(rci_class_static  = 3, F.weight, 0)),2) AS portions_3,
      ROUND(SUM(IF(rci_class_static  = 4, F.weight, 0)),2) AS portions_4,
      ROUND(SUM(IF(rci_class_static  = 5, F.weight, 0)),2) AS portions_5,
      ROUND(SUM(IF(rci_class_static  = 6, F.weight, 0)),2) AS portions_6,

      COUNTIF(rci_class_static <= 1) AS rci_low , -- tally of papers in classes 0,1 using FoR fractions
      COUNTIF(rci_class_static >= 4) AS rci_high, -- tally of papers in classes 4,5,6 using FoR fractions
      0.0                            AS rci_ratio -- ratio of rci_high to rci_low

      --hep_for_avg  , -- the percentage of papers (from all HEPs) in this class
      --hep_for_pct  , -- the percentage of papers (from the HEP) of all HEPs' papers in this class
    FROM \`${project}.${dataset}.core_papers${version}\` AS A
    INNER JOIN UNNEST(insts) AS I
    INNER JOIN UNNEST(concs) AS F
    LEFT JOIN \`${project}.${dataset}.rci_classes_papers${version}\` AS B ON A.doi = B.doi
    GROUP BY inst,conc
    ORDER BY inst,conc
  );
  UPDATE \`${project}.${dataset}.rci_classes_summary${version}\` SET rci_ratio = IF(rci_low=0, rci_high, rci_high / rci_low) WHERE true;
END;`;
