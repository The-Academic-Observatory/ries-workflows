/*
## Summary
Compiles data for the ERA research outputs indicator.

## Description
For methodology, see:
https://github.com/Curtin-Open-Knowledge-Initiative/coki-ries/blob/main/docs/era_2018.md#indicator-research-outputs
https://github.com/Curtin-Open-Knowledge-Initiative/coki-ries/blob/main/docs/era_2018.md#indicator-research-outputs-by-year

When all combinations are used, this will produce 32 different tables.

## Contacts
julian.tonti-filippini@curtin.edu.au

## License
Apache 2.0

## Requires
table core_assignments

## Creates
table research_outputs_*
*/
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
  ].filter((v) => v);
  let table = `${project}.${dataset}.research_outputs_${scope}_${digits}_${group.join("_")}${version}`;
  return `
-- generated by: ${require("path").basename(__filename)}
BEGIN
  -- create outputs table ${scope} ${digits}digit (${group})
  ${replace ? "CREATE OR REPLACE TABLE" : "CREATE TABLE IF NOT EXISTS"} \`${table}\` AS (
    SELECT
      ${group.join(",")},
      COUNT(1)  AS sum_papers,
      SUM(cits) AS sum_citations,
      SUM(frac) AS sum_portions,
      AVG(cits) AS avg_citations,
      NTILE(100) OVER (PARTITION BY field,year ORDER BY COUNT(1)  DESC) AS cent_papers,
      NTILE(100) OVER (PARTITION BY field,year ORDER BY SUM(cits) DESC) AS cent_citations,
      NTILE(100) OVER (PARTITION BY field,year ORDER BY SUM(frac) DESC) AS cent_portions,
      NTILE(100) OVER (PARTITION BY field,year ORDER BY AVG(cits) DESC) AS cent_cpp,
      RANK()     OVER (PARTITION BY field,year ORDER BY COUNT(1)  DESC) AS rank_papers,
      RANK()     OVER (PARTITION BY field,year ORDER BY SUM(cits) DESC) AS rank_citations,
      RANK()     OVER (PARTITION BY field,year ORDER BY SUM(frac) DESC) AS rank_portions,
      RANK()     OVER (PARTITION BY field,year ORDER BY AVG(cits) DESC) AS rank_cpp
    FROM (
      SELECT
        ${institution ? "inst" : "null"} AS institution,
        ${field ? "field" : "null"} AS field,
        ${year ? "year" : "null"} AS year,
        ANY_VALUE(frac) AS frac,
        ANY_VALUE(cits) AS cits
      FROM \`${project}.${dataset}.core_assignments${version}\`
      WHERE LENGTH(field) = ${digits} ${scope == "local" ? "AND is_hep" : ""}
      GROUP BY paper,institution,field,year
    )
    -- this works because of the ternary operations in the internal select
    GROUP BY institution,field,year
    ORDER BY institution,field,year
  );
  -- sanity checks
  SELECT 'test', COUNT(1) AS total, ${group.map((g) => `COUNT(DISTINCT ${g}) AS num_${g}s`).join(",")} FROM \`${table}\`;
END;
`;
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

// this is a simpler form of the query that assumes pre-filtering of the institutions and concepts.
const compile2 = ({
  project = "",
  dataset = "",
  inst = true,
  conc = true,
  year = true,
  replace = false,
  version = "",
}) => {
  let group = [inst ? "inst" : "", conc ? "conc" : "", year ? "year" : ""]
    .filter((v) => v)
    .join(",");
  let table = `${project}.${dataset}.research_outputs_${group.split(",").join("_")}${version}`;
  return `
-- generated by: ${require("path").basename(__filename)}
BEGIN
  -- create outputs table (${group})
  ${replace ? "CREATE OR REPLACE TABLE" : "CREATE TABLE IF NOT EXISTS"} \`${table}\` AS (
    SELECT
      ${group},
      COUNT(1)  AS sum_papers,
      SUM(cits) AS sum_citations,
      SUM(frac) AS sum_portions,
      AVG(cits) AS avg_citations,
      NTILE(100) OVER (PARTITION BY field,year ORDER BY COUNT(1)  DESC) AS cent_papers,
      NTILE(100) OVER (PARTITION BY field,year ORDER BY SUM(cits) DESC) AS cent_citations,
      NTILE(100) OVER (PARTITION BY field,year ORDER BY SUM(frac) DESC) AS cent_portions,
      NTILE(100) OVER (PARTITION BY field,year ORDER BY AVG(cits) DESC) AS cent_cpp,
      RANK()     OVER (PARTITION BY field,year ORDER BY COUNT(1)  DESC) AS rank_papers,
      RANK()     OVER (PARTITION BY field,year ORDER BY SUM(cits) DESC) AS rank_citations,
      RANK()     OVER (PARTITION BY field,year ORDER BY SUM(frac) DESC) AS rank_portions,
      RANK()     OVER (PARTITION BY field,year ORDER BY AVG(cits) DESC) AS rank_cpp
    FROM (
      SELECT
        ${group},
        ANY_VALUE(frac) AS frac,
        ANY_VALUE(cits) AS cits
      FROM \`${project}.${dataset}.core_assignments${version}\`
      GROUP BY work,${group}
    )
    -- this works because of the ternary operations in the internal select
    GROUP BY inst,conc,year
    ORDER BY inst,conc,year
  );
  -- sanity checks
  SELECT 'test', COUNT(1) AS total, ${group
    .split(",")
    .map((g) => `COUNT(DISTINCT ${g}) AS num_${g}s`)
    .join(",")} FROM \`${table}\`;
END;
`;
};
