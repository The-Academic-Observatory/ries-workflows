/*
Requires:
    core_assignments
    heps
    heps_papers
    doi

Outputs:
    heps_outputs
*/

const {
  check_assignment,
  check_field,
  check_string,
  check_hep_ror,
  check_hep_code,
} = require("../libraries/lib_checks");
const { hep_map } = require("../libraries/lib_hep_map");

function compile_automatic({
  project = "",
  dataset = "",
  assignment = "",
  version = "",
  doi_table = "",
  start = 2011,
  finish = 2022,
}) {
  return `
BEGIN
CREATE OR REPLACE TABLE \`${project}.${dataset}.heps_outputs${version}\` AS (
  WITH 
  -- extract HEP authors from Open Alex data
  alex_hep AS (
    SELECT 
      doi,
      ror,
      ARRAY_AGG(STRUCT(
        authorship.author.display_name AS name,
        authorship.author.orcid AS orcid,
        authorship.raw_affiliation_strings AS raw_affiliation
      )) AS hep_authors
    FROM ${doi_table}, UNNEST(openalex.authorships) AS authorship, UNNEST(institutions)
    WHERE EXISTS(SELECT 1 from UNNEST(authorship.institutions))
    GROUP BY doi, ror
  ),

  alex_all AS (
    SELECT 
      doi            AS doi,
      openalex.id    AS id,
      openalex.title AS paper_title,
      openalex.primary_location.source.display_name AS journal_title,
      ARRAY_AGG(STRUCT(
        authorship.author.display_name AS name,
        authorship.author.orcid AS orcid,
        authorship.raw_affiliation_strings AS raw_affiliation
      )) AS authors
  FROM ${doi_table}
  LEFT JOIN UNNEST(openalex.authorships) AS authorship
  GROUP BY doi, id, paper_title, journal_title
  ),

  alex AS (
    SELECT 
      alex_all.*,
      hep.hep_authors AS hep_authors,
      hep.ror AS hep_ror
      FROM alex_all AS alex_all
      LEFT JOIN alex_hep AS hep ON alex_all.doi = hep.doi
  ),

  assignments AS (
    SELECT 
      paper AS doi,
      inst AS ror,
      ARRAY_AGG(STRUCT(
        REPLACE(field, "MD", "99") AS code,
        frac                       AS weight
      )) AS apportionment
    FROM \`${project}.${dataset}.core_assignments${version}\` 
    GROUP BY doi, ror
  )
  
  -- extract the HEP outputs identified by the RIES automatic by-line approach
  SELECT
    CONCAT('https://doi.org/', ries.doi) AS doi,
    alex.id                              AS openalex_id,
    ries.era_id                          AS journal_era_id,
    alex.journal_title                   AS journal_title,
    alex.paper_title                     AS paper_title,
    alex.authors                         AS authors,
    alex.hep_authors                     AS hep_authors,
    ries.year                            AS year,
    ries.citations                       AS citations,
    assignments.apportionment            AS apportionment,
    ries.institution                     AS hep_ror,
    all_heps.code                        AS hep_code,
    '${assignment}'                      AS assignment
  FROM \`${project}.${dataset}.heps_papers${version}\` AS ries
  LEFT JOIN alex ON ries.doi = alex.doi AND alex.hep_ror = ries.institution
  LEFT JOIN assignments ON ries.doi = assignments.doi AND ries.institution = assignments.ror
  LEFT JOIN \`${project}.${dataset}.heps${version}\` AS all_heps ON all_heps.ror = ries.institution
  WHERE year >= ${start} AND year <= ${finish} 
  ORDER BY year, doi
);
ALTER TABLE \`${project}.${dataset}.heps_outputs${version}\` SET OPTIONS(description='HEP outputs for fields of education identified by the RIES by-line approach.');
ALTER TABLE \`${project}.${dataset}.heps_outputs${version}\`
  ALTER COLUMN doi            SET OPTIONS(description="Full link to the Digitial Object Identifier of the paper"),
  ALTER COLUMN openalex_id    SET OPTIONS(description="The OpenAlex identifier"),
  ALTER COLUMN journal_era_id SET OPTIONS(description="ERA Journal List ID of the encompassing journal"),
  ALTER COLUMN journal_title  SET OPTIONS(description="Journal Title (from the ERA Journal List"),
  ALTER COLUMN paper_title    SET OPTIONS(description="Journal article title obtained from OpenAlex"),
  ALTER COLUMN authors        SET OPTIONS(description="All authors obtained from OpenAlex"),
  ALTER COLUMN hep_authors    SET OPTIONS(description="All HEP authors obtained from OpenAlex"),
  ALTER COLUMN year           SET OPTIONS(description="Year of publication"),
  ALTER COLUMN citations      SET OPTIONS(description="Current number of citations"),
  ALTER COLUMN apportionment  SET OPTIONS(description="Research topic codes assigned to the paper with portions/weights. Each set of codes should sum to 100 portions at the 2-digit and 4-digit levels"),
  ALTER COLUMN assignment     SET OPTIONS(description="Assignment method"),
  ALTER COLUMN hep_ror        SET OPTIONS(description="The ROR for this work's HEP"),
  ALTER COLUMN hep_code       SET OPTIONS(description="The code for this work's HEP")
      ;
END;
`;
}

function compile_institutional({
  project = "",
  dataset = "",
  assignment = "",
  version = "",
  hep_code = "",
  doi_table = "",
  start = 2011,
  finish = 2022,
}) {
  hep_ror = hep_map[hep_code].ror;
  check_hep_ror(hep_ror);
  return `
    BEGIN
CREATE OR REPLACE TABLE \`${project}.${dataset}.heps_outputs${version}\` AS (
  WITH 
  -- extract HEP authors from Open Alex data
  alex_hep AS (
    SELECT 
      doi,
      ARRAY_AGG(STRUCT(
        authorship.author.display_name AS name,
        authorship.author.orcid AS orcid,
        authorship.raw_affiliation_strings AS raw_affiliation
      )) AS hep_authors
    FROM  ${doi_table}, 
    UNNEST(openalex.authorships) AS authorship 
    WHERE EXISTS(SELECT 1 from UNNEST(authorship.institutions) WHERE ror = '${hep_ror}')
    GROUP BY doi
  ),

  alex_all AS (
    SELECT 
      doi            AS doi,
      openalex.id    AS id,
      openalex.title AS paper_title,
      openalex.primary_location.source.display_name AS journal_title,
      ARRAY_AGG(STRUCT(
        authorship.author.display_name AS name,
        authorship.author.orcid AS orcid,
        authorship.raw_affiliation_strings AS raw_affiliation
      )) AS authors
  FROM ${doi_table}
  LEFT JOIN UNNEST(openalex.authorships) AS authorship
  GROUP BY doi, id, paper_title, journal_title
  ),

  alex AS (
    SELECT 
      alex_all.*,
      hep.hep_authors AS hep_authors,
      FROM alex_all AS alex_all
      LEFT JOIN alex_hep AS hep ON alex_all.doi = hep.doi
  ),

  assignments AS (
    SELECT 
      paper AS doi,
      ARRAY_AGG(STRUCT(
        REPLACE(field, "MD", "99") AS code,
        frac                       AS weight
      )) AS apportionment
    FROM \`${project}.${dataset}.core_assignments${version}\` WHERE inst = '${hep_ror}'
    GROUP BY doi
  )
  
  -- extract the HEP outputs from the RIES workflow
  SELECT
    CONCAT('https://doi.org/', ries.doi) AS doi,
    alex.id                              AS openalex_id,
    ries.era_id                          AS journal_era_id,
    alex.journal_title                   AS journal_title,
    alex.paper_title                     AS paper_title,
    alex.authors                         AS authors,
    alex.hep_authors                     AS hep_authors,
    ries.year                            AS year,
    ries.citations                       AS citations,
    assignments.apportionment            AS apportionment,
    ries.institution                     AS hep_ror,
    all_heps.code                        AS hep_code,
    '${assignment}'                      AS assignment
  FROM \`${project}.${dataset}.heps_papers${version}\` AS ries
  LEFT JOIN alex ON ries.doi = alex.doi 
  LEFT JOIN assignments ON ries.doi = assignments.doi 
  LEFT JOIN \`${project}.${dataset}.heps${version}\` AS all_heps ON all_heps.ror = ries.institution
  WHERE year >= ${start} AND year <= ${finish} AND institution = '${hep_ror}'
  ORDER BY year, doi
);
  ALTER TABLE \`${project}.${dataset}.heps_outputs${version}\` SET OPTIONS(description='HEP outputs for fields of education identified by the RIES by-line approach.');
  ALTER TABLE \`${project}.${dataset}.heps_outputs${version}\`
  ALTER COLUMN doi            SET OPTIONS(description="Full link to the Digitial Object Identifier of the paper"),
  ALTER COLUMN openalex_id    SET OPTIONS(description="The OpenAlex identifier"),
  ALTER COLUMN journal_era_id SET OPTIONS(description="ERA Journal List ID of the encompassing journal"),
  ALTER COLUMN journal_title  SET OPTIONS(description="Journal Title (from the ERA Journal List"),
  ALTER COLUMN paper_title    SET OPTIONS(description="Journal article title obtained from OpenAlex"),
  ALTER COLUMN authors        SET OPTIONS(description="All authors obtained from OpenAlex"),
  ALTER COLUMN hep_authors    SET OPTIONS(description="All HEP authors obtained from OpenAlex"),
  ALTER COLUMN year           SET OPTIONS(description="Year of publication"),
  ALTER COLUMN citations      SET OPTIONS(description="Current number of citations"),
  ALTER COLUMN apportionment  SET OPTIONS(description="Research topic codes assigned to the paper with portions/weights. Each set of codes should sum to 100 portions at the 2-digit and 4-digit levels"),
  ALTER COLUMN assignment     SET OPTIONS(description="Assignment method"),
  ALTER COLUMN hep_ror        SET OPTIONS(description="The ROR for this work's HEP"),
  ALTER COLUMN hep_code       SET OPTIONS(description="The code for this work's HEP")
      ;
END;
`;
}

function compile(
  conf = {
    project: "",
    dataset: "",
    assignment: "",
    version: "",
    doi_table: "",
    hep_code: "", // Optional - Unused for institutional
    start: 2011,
    finish: 2022,
  },
) {
  check_assignment(conf.assignment);
  check_field(conf.field);
  check_string(conf.doi_table);
  check_string(conf.dataset);
  check_string(conf.project);
  if (conf.assignment === "automatic") {
    return compile_automatic(conf);
  } else if ((conf.assignment = "institutional")) {
    check_hep_code(conf.hep_code);
    return compile_institutional(conf);
  }
}

module.exports = { compile };
