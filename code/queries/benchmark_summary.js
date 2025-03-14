/*
## Summary
Compiles a summary table with all benchmark data in one place by (field,year).

## Description
This joins together:
- local benchmark rci
- world benchmark rci
- world benchmark hpi
- rci centile boundaries
- rci static boundaries
- rci dynamic boundaries

## Contacts
julian.tonti-filippini@curtin.edu.au

## License
Apache 2.0

## Requires
table benchmarks_cpp_*
table benchmarks_hpi_*
table benchmarks_centiles_*
table benchmarks_rci_*
table core_fors

## Creates
table benchmarks_summary_*
*/
const compile = ({
  project = "",
  dataset = "",
  digits = 4,
  replace = false,
  version = "",
}) => `
-- generated by: ${require("path").basename(__filename)}
BEGIN
  -- the summary table brings together all class thresholds and benchmarks
  ${replace ? "CREATE OR REPLACE TABLE" : "CREATE TABLE IF NOT EXISTS"} \`${project}.${dataset}.benchmarks_summary_${digits}_${version}\` AS (
    SELECT 
      A.code                 AS field,
      F.name                 AS name, 
      A.year                 AS year,
      A.num_papers           AS num_papers,
      A.num_uncited          AS num_uncited,
      ROUND(A.benchmark , 4) AS cpp_local,
      ROUND(B.benchmark , 4) AS cpp_world,
      ROUND(C.benchmark , 4) AS cpp_hpi,
      ROUND(D.c1        , 4) AS ctile_01,
      ROUND(D.c5        , 4) AS ctile_05,
      ROUND(D.c10       , 4) AS ctile_10,
      ROUND(D.c25       , 4) AS ctile_25,
      ROUND(D.c50       , 4) AS ctile_50,
      ROUND(E.d_c0      , 4) AS dynamic_c0,
      ROUND(E.d_c1      , 4) AS dynamic_c1,
      ROUND(E.d_c2      , 4) AS dynamic_c2,
      ROUND(E.d_c3      , 4) AS dynamic_c3,
      ROUND(E.d_c4      , 4) AS dynamic_c4,
      'INF'                  AS dynamic_c5,
      --ROUND(E.d_c5      , 4) AS dynamic_c5,
      E.max_rci              AS maximum_rci,
      ROUND(E.s_c0      , 4) AS static_c0,
      ROUND(E.s_c1      , 4) AS static_c1,
      ROUND(E.s_c2      , 4) AS static_c2,
      ROUND(E.s_c3      , 4) AS static_c3,
      ROUND(E.s_c4      , 4) AS static_c4,
      ROUND(E.s_c5      , 4) AS static_c5,
      'INF'                  AS static_c6
      --ROUND(E.s_c6      , 4) AS static_c6
    FROM      \`${project}.${dataset}.benchmarks_cpp_local_${digits}_${version}\`      AS A
    LEFT JOIN \`${project}.${dataset}.benchmarks_cpp_world_${digits}_${version}\`      AS B ON A.code = B.code  AND A.year = B.year
    LEFT JOIN \`${project}.${dataset}.benchmarks_hpi_world_${digits}_${version}\`      AS C ON A.code = C.field AND A.year = C.year
    LEFT JOIN \`${project}.${dataset}.benchmarks_centiles_world_${digits}_${version}\` AS D ON A.code = D.field AND A.year = D.year
    LEFT JOIN \`${project}.${dataset}.benchmarks_rci_world_${digits}_${version}\`      AS E ON A.code = E.field AND A.year = E.year
    LEFT JOIN \`${project}.${dataset}.core_fors${version}\` AS F ON a.code = F.code
    ORDER BY field,year
  );
  ALTER TABLE \`${project}.${dataset}.benchmarks_summary_${digits}_${version}\` SET OPTIONS(description='Aggregation of all benchmark and class boundary values by field of research and year');
  ALTER TABLE ${project}.${dataset}.benchmarks_summary_${digits}_${version}
  ALTER COLUMN field       SET OPTIONS (description="ANZSIC field of research code"),
  ALTER COLUMN name        SET OPTIONS (description="ANZSIC field of research name"),
  ALTER COLUMN year        SET OPTIONS (description="assessment year (publication year for papers)"),
  ALTER COLUMN num_papers  SET OPTIONS (description="total number of papers published in the year"),
  ALTER COLUMN num_uncited SET OPTIONS (description="number of papers that have no citations"),
  ALTER COLUMN cpp_local   SET OPTIONS (description="benchmark citations per paper for Australian HEPs only"),
  ALTER COLUMN cpp_world   SET OPTIONS (description="benchmark citations per paper for all institutions"),
  ALTER COLUMN cpp_hpi     SET OPTIONS (description="benchmark citations per paper for high performing global insitutions (avg of the top 10%)"),
  ALTER COLUMN ctile_01    SET OPTIONS (description="citations needed to be in the top 1% globally"),
  ALTER COLUMN ctile_05    SET OPTIONS (description="citations needed to be in the top 5% globally"),
  ALTER COLUMN ctile_10    SET OPTIONS (description="citations needed to be in the top 10% globally"),
  ALTER COLUMN ctile_25    SET OPTIONS (description="citations needed to be in the top 25% globally"),
  ALTER COLUMN ctile_50    SET OPTIONS (description="citations needed to be in the top 50% globally"),
  ALTER COLUMN dynamic_c0  SET OPTIONS (description="RCI score upper limit for category 0 (dynamic method)"),
  ALTER COLUMN dynamic_c1  SET OPTIONS (description="RCI score upper limit for category 1 (dynamic method)"),
  ALTER COLUMN dynamic_c2  SET OPTIONS (description="RCI score upper limit for category 2 (dynamic method)"),
  ALTER COLUMN dynamic_c3  SET OPTIONS (description="RCI score upper limit for category 3 (dynamic method)"),
  ALTER COLUMN dynamic_c4  SET OPTIONS (description="RCI score upper limit for category 4 (dynamic method)"),
  ALTER COLUMN dynamic_c5  SET OPTIONS (description="RCI score upper limit for category 5 (dynamic method)"),
  ALTER COLUMN maximum_rci SET OPTIONS (description="The maximum observed RCI score (technically the precise upper limit for dynamic_c5)"),
  ALTER COLUMN static_c0   SET OPTIONS (description="RCI score upper limit for category 0 (static)"),
  ALTER COLUMN static_c1   SET OPTIONS (description="RCI score upper limit for category 1 (static)"),
  ALTER COLUMN static_c2   SET OPTIONS (description="RCI score upper limit for category 2 (static)"),
  ALTER COLUMN static_c3   SET OPTIONS (description="RCI score upper limit for category 3 (static)"),
  ALTER COLUMN static_c4   SET OPTIONS (description="RCI score upper limit for category 4 (static)"),
  ALTER COLUMN static_c5   SET OPTIONS (description="RCI score upper limit for category 5 (static)"),
  ALTER COLUMN static_c6   SET OPTIONS (description="RCI score upper limit for category 6 (static)");
END;
`;
const compile_all = (args = {}) => [
  compile({ ...args, digits: 4 }),
  compile({ ...args, digits: 2 }),
];
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
  -- the summary table brings together all class thresholds and benchmarks
  ${replace ? "CREATE OR REPLACE TABLE" : "CREATE TABLE IF NOT EXISTS"} \`${project}.${dataset}.benchmarks_summary${version}\` AS (
    SELECT 
      A.code                 AS conc,
      F.name                 AS name,
      A.year                 AS year,
      A.num_papers           AS num_papers,
      A.num_uncited          AS num_uncited,
      ROUND(A.benchmark , 4) AS benchmark_cpp,
      ROUND(C.benchmark , 4) AS benchmark_hpi,
      ROUND(D.c1        , 4) AS ctile_01,
      ROUND(D.c5        , 4) AS ctile_05,
      ROUND(D.c10       , 4) AS ctile_10,
      ROUND(D.c25       , 4) AS ctile_25,
      ROUND(D.c50       , 4) AS ctile_50,
      ROUND(E.d_c0      , 4) AS dynamic_c0,
      ROUND(E.d_c1      , 4) AS dynamic_c1,
      ROUND(E.d_c2      , 4) AS dynamic_c2,
      ROUND(E.d_c3      , 4) AS dynamic_c3,
      ROUND(E.d_c4      , 4) AS dynamic_c4,
      'INF'                  AS dynamic_c5,
      --ROUND(E.d_c5      , 4) AS dynamic_c5,
      E.max_rci              AS maximum_rci,
      ROUND(E.s_c0      , 4) AS static_c0,
      ROUND(E.s_c1      , 4) AS static_c1,
      ROUND(E.s_c2      , 4) AS static_c2,
      ROUND(E.s_c3      , 4) AS static_c3,
      ROUND(E.s_c4      , 4) AS static_c4,
      ROUND(E.s_c5      , 4) AS static_c5,
      'INF'                  AS static_c6
      --ROUND(E.s_c6      , 4) AS static_c6
    FROM      \`${project}.${dataset}.benchmarks_cpp${version}\`      AS A
    LEFT JOIN \`${project}.${dataset}.benchmarks_hpi${version}\`      AS C ON A.code = C.field AND A.year = C.year
    LEFT JOIN \`${project}.${dataset}.benchmarks_centiles${version}\` AS D ON A.code = D.field AND A.year = D.year
    LEFT JOIN \`${project}.${dataset}.benchmarks_rci${version}\`      AS E ON A.code = E.field AND A.year = E.year
    LEFT JOIN \`${project}.${dataset}.core_concs${version}\`          AS F ON A.code = F.code
    ORDER BY conc,year
  );
  ALTER TABLE \`${project}.${dataset}.benchmarks_summary${version}\` SET OPTIONS(description='Aggregation of all benchmark and class boundary values by field of research and year');
  ALTER TABLE ${project}.${dataset}.benchmarks_summary${version}
  ALTER COLUMN field         SET OPTIONS (description="ANZSIC field of research code"),
  ALTER COLUMN name          SET OPTIONS (description="ANZSIC field of research name"),
  ALTER COLUMN year          SET OPTIONS (description="assessment year (publication year for papers)"),
  ALTER COLUMN num_papers    SET OPTIONS (description="total number of papers published in the year"),
  ALTER COLUMN num_uncited   SET OPTIONS (description="number of papers that have no citations"),
  ALTER COLUMN benchmark_cpp SET OPTIONS (description="benchmark citations per paper for all institutions"),
  ALTER COLUMN benchmark_hpi SET OPTIONS (description="benchmark citations per paper for high performing institutions (avg of the top 10%)"),
  ALTER COLUMN ctile_01      SET OPTIONS (description="citations needed to be in the top 1% globally"),
  ALTER COLUMN ctile_05      SET OPTIONS (description="citations needed to be in the top 5% globally"),
  ALTER COLUMN ctile_10      SET OPTIONS (description="citations needed to be in the top 10% globally"),
  ALTER COLUMN ctile_25      SET OPTIONS (description="citations needed to be in the top 25% globally"),
  ALTER COLUMN ctile_50      SET OPTIONS (description="citations needed to be in the top 50% globally"),
  ALTER COLUMN dynamic_c0    SET OPTIONS (description="RCI score upper limit for category 0 (dynamic method)"),
  ALTER COLUMN dynamic_c1    SET OPTIONS (description="RCI score upper limit for category 1 (dynamic method)"),
  ALTER COLUMN dynamic_c2    SET OPTIONS (description="RCI score upper limit for category 2 (dynamic method)"),
  ALTER COLUMN dynamic_c3    SET OPTIONS (description="RCI score upper limit for category 3 (dynamic method)"),
  ALTER COLUMN dynamic_c4    SET OPTIONS (description="RCI score upper limit for category 4 (dynamic method)"),
  ALTER COLUMN dynamic_c5    SET OPTIONS (description="RCI score upper limit for category 5 (dynamic method)"),
  ALTER COLUMN maximum_rci   SET OPTIONS (description="The maximum observed RCI score (technically the precise upper limit for dynamic_c5)"),
  ALTER COLUMN static_c0     SET OPTIONS (description="RCI score upper limit for category 0 (static)"),
  ALTER COLUMN static_c1     SET OPTIONS (description="RCI score upper limit for category 1 (static)"),
  ALTER COLUMN static_c2     SET OPTIONS (description="RCI score upper limit for category 2 (static)"),
  ALTER COLUMN static_c3     SET OPTIONS (description="RCI score upper limit for category 3 (static)"),
  ALTER COLUMN static_c4     SET OPTIONS (description="RCI score upper limit for category 4 (static)"),
  ALTER COLUMN static_c5     SET OPTIONS (description="RCI score upper limit for category 5 (static)"),
  ALTER COLUMN static_c6     SET OPTIONS (description="RCI score upper limit for category 6 (static)");
END;
`;
