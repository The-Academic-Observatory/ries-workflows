/*
## Summary
Assignments are the atomic unit of analysis. It represents an element of academic output with a percentage weighting. The primary key is a composite key of work (doi), institution (ror) and concept (code). The value is a percentage weighting. Weights must sum to 100% when merged on work and institution.

## Description
There are several ways that assignments may be determined. They may be automatically generated or manually curated. Connections between works and institutions are typically extracted from the COKI DOI table. Connections between works and concepts may be inherited from journal-level data or from work-level data.

Future development (RIES2) is intended to make this process more flexible. Currently (RIES1) the assignment process is automatically determined against the ERA Journal List(s). This list was constructed externally, but the ARC, and assigns research concepts (with weights) at the journal level. In RIES1, a work is only included if it is published within one of these journals. The work then inherits the exact assignment made at the journal level.

Based on subsequent work with UNE, if an Australian HEP can provide manually curated assignments, then this information can be used to patch the automatically generated data, to enhance accuracy for that institution.

## Contacts
julian.tonti-filippini@curtin.edu.au

## License
Apache 2.0

## Requires
table core_papers
table core_heps

## Creates
table core_assignments

*/
function compile({
  project = "",
  dataset = "",
  replace = false,
  version = "",
}) {
  return `
-- generated by: ${require("path").basename(__filename)}
BEGIN
  -- base table that the rest of the tables build from
  ${replace ? "CREATE OR REPLACE TABLE" : "CREATE TABLE IF NOT EXISTS"} \`${project}.${dataset}.core_assignments${version}\` AS (
    SELECT 
      A.year_published  AS year,
      A.era_id          AS journal,
      A.doi             AS paper,
      A.num_citations   AS cits,
      ror               AS inst,
      NOT B.ror IS NULL AS is_hep, -- flag set to true if the institution is an Australian HEP
      fc.code           AS field,
      SUBSTRING(fc.code,0,2) AS field2, -- two digit FoR code
      fc.weight         AS frac
    FROM \`${project}.${dataset}.core_papers${version}\` AS A
    LEFT JOIN UNNEST(rors) AS ror
    LEFT JOIN UNNEST(fors) AS fc
    LEFT JOIN \`${project}.${dataset}.core_heps${version}\` AS B ON ror = B.ror
    ORDER BY inst,field,year
  );
  SELECT 
    'sanity checking counts in base table',
    COUNT(DISTINCT paper)   AS unique_papers,
    COUNT(DISTINCT inst)    AS unique_institutions,
    COUNT(DISTINCT journal) AS unique_journals,
    COUNT(DISTINCT field)   AS unique_fields,
    COUNT(DISTINCT year)    AS unique_years,
    COUNT(1)                AS total_rows
  FROM ${project}.${dataset}.core_assignments${version};
END;
`;
}
const compile_all = (args = {}) => [compile(args)];
module.exports = { compile, compile_all };
if (require.main === module) require("app").cli_compile(compile_all);
