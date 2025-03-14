/*
## Summary
Assigns RCI scores (using various benchmarks) to groupings of papers, weighted by FoR apportionment.

## Description
Method: https://github.com/Curtin-Open-Knowledge-Initiative/coki-ries/blob/main/docs/era_2018.md#indicator-relative-citation-impact-rci

Unlike individual papers, groupings use the weighted RCI scores of grouped papers to determine a higher level RCI. Papers use only citation count.

## Contacts
julian.tonti-filippini@curtin.edu.au

## License
Apache 2.0

## Requires
table core_papers
table rci_papers

## Creates
table rci_grouping_*
*/
const compile_old = ({
  project = "",
  dataset = "",
  scope = "world",
  digits = 4,
  institution = true,
  field = true,
  year = true,
  replace = false,
  version = "20240801",
}) => {
  let group = [
    institution ? "institution" : "",
    field ? "field" : "",
    year ? "year" : "",
  ]
    .filter((v) => v)
    .join(",");
  let table = `${project}.${dataset}.rci_grouping_${scope}_${digits}_${group.split(",").join("_")}${version}`;
  return `
-- generated by: ${require("path").basename(__filename)}
BEGIN
  -- assign RCI scores to each (${group}) using apportionment and a weighted average
  ${replace ? "CREATE OR REPLACE TABLE" : "CREATE TABLE IF NOT EXISTS"} ${table} AS (
    SELECT 
      ${group},
      IF (SUM(weight) = 0, 0, SUM(rci_local * weight) / SUM(weight)) AS rci_local,
      IF (SUM(weight) = 0, 0, SUM(rci_world * weight) / SUM(weight)) AS rci_world,
      IF (SUM(weight) = 0, 0, SUM(hpi_world * weight) / SUM(weight)) AS hpi_world
    FROM (
      SELECT 
        ror            AS institution,
        fc.code        AS field,
        fc.weight      AS weight,
        year_published AS year,
        B.rci_local    AS rci_local,
        B.rci_world    AS rci_world,
        B.hpi_world    AS hpi_world
      FROM \`${project}.${dataset}.core_papers${version}${version}\` AS A
      LEFT JOIN UNNEST(rors) AS ror
      LEFT JOIN UNNEST(fors) AS fc
      LEFT JOIN \`${project}.${dataset}.rci_papers${version}\` AS B ON A.doi = B.doi AND fc.code = B.field AND fc.len = ${digits} -- TODO: check this digits condition, added but not tested yet
      WHERE LENGTH(field) = ${digits}
    )
    GROUP BY ${group}
    ORDER BY ${group}
  );
END;`;
};

const compile = ({
  project = "",
  dataset = "",
  scope = "world",
  digits = 4,
  institution = true,
  field = true,
  year = true,
  replace = false,
  version = "",
}) => {
  let group = [
    institution ? "institution" : "",
    field ? "field" : "",
    year ? "year" : "",
  ]
    .filter((v) => v)
    .join(",");
  let table = `${project}.${dataset}.rci_grouping_${scope}_${digits}_${group.split(",").join("_")}${version}`;
  return `
-- generated by: ${require("path").basename(__filename)}
BEGIN
  -- assign RCI scores to each (${group}) using apportionment and a weighted average
  ${replace ? "CREATE OR REPLACE TABLE" : "CREATE TABLE IF NOT EXISTS"} ${table} AS (
    SELECT 
      ${group},
      IF (SUM(weight) = 0, 0, SUM(rci_local * weight) / SUM(weight)) AS rci_local,
      IF (SUM(weight) = 0, 0, SUM(rci_world * weight) / SUM(weight)) AS rci_world,
      IF (SUM(weight) = 0, 0, SUM(hpi_world * weight) / SUM(weight)) AS hpi_world
    FROM (
      SELECT 
        A.inst      AS institution,
        A.field     AS field,
        A.frac      AS weight,
        A.year      AS year,
        B.rci_local AS rci_local,
        B.rci_world AS rci_world,
        B.hpi_world AS hpi_world
      FROM \`${project}.${dataset}.core_assignments${version}\` AS A
      LEFT JOIN \`${project}.${dataset}.rci_papers${version}\` AS B ON A.paper = B.doi AND A.field = B.field AND LENGTH(A.field) = ${digits} -- TODO: check this digits condition, added but not tested yet
      WHERE LENGTH(A.field) = ${digits}
    )
    GROUP BY ${group}
    ORDER BY ${group}
  );
END;`;
};
function compile_all(args = {}) {
  const sqls = [];
  ["world", "local"].forEach((scope) =>
    [4, 2].forEach((digits) => {
      sqls.push(
        compile({
          ...args,
          scope,
          digits,
          institution: true,
          field: true,
          year: true,
        }),
      );
      sqls.push(
        compile({
          ...args,
          scope,
          digits,
          institution: true,
          field: true,
          year: false,
        }),
      );
      sqls.push(
        compile({
          ...args,
          scope,
          digits,
          institution: true,
          field: false,
          year: true,
        }),
      );
      sqls.push(
        compile({
          ...args,
          scope,
          digits,
          institution: false,
          field: true,
          year: true,
        }),
      );
      sqls.push(
        compile({
          ...args,
          scope,
          digits,
          institution: true,
          field: false,
          year: false,
        }),
      );
      sqls.push(
        compile({
          ...args,
          scope,
          digits,
          institution: false,
          field: true,
          year: false,
        }),
      );
      sqls.push(
        compile({
          ...args,
          scope,
          digits,
          institution: false,
          field: false,
          year: true,
        }),
      );
    }),
  );
  return sqls;
}
module.exports = { compile, compile_all };
if (require.main === module) require("app").cli_compile(compile_all);

const compile2 = ({
  project = "",
  dataset = "",
  inst = true,
  conc = true,
  year = true,
  replace = false,
  version = "20240801",
}) => {
  let group = [inst ? "inst" : "", conc ? "conc" : "", year ? "year" : ""]
    .filter((v) => v)
    .join(",");
  let table = `${project}.${dataset}.rci_grouping_${group.split(",").join("_")}${version}`;
  return `
-- generated by: ${require("path").basename(__filename)}
BEGIN
  -- assign RCI scores to each (${group}) using apportionment and a weighted average
  ${replace ? "CREATE OR REPLACE TABLE" : "CREATE TABLE IF NOT EXISTS"} ${table} AS (
    SELECT 
      ${group},
      IF (SUM(weight) = 0, 0, SUM(rci * weight) / SUM(weight)) AS rci,
      IF (SUM(weight) = 0, 0, SUM(hpi * weight) / SUM(weight)) AS hpi
    FROM (
      SELECT 
        ror            AS inst,
        fc.code        AS conc,
        fc.weight      AS weight,
        year_published AS year,
        B.rci          AS rci,
        B.hpi          AS hpi
      FROM \`${project}.${dataset}.core_papers${version}\` AS A
      LEFT JOIN UNNEST(insts) AS ror
      LEFT JOIN UNNEST(concs) AS fc
      LEFT JOIN \`${project}.${dataset}.rci_papers${version}\` AS B ON A.doi = B.doi AND fc.code = B.conc
    )
    GROUP BY ${group}
    ORDER BY ${group}
  );
END;`;
};
