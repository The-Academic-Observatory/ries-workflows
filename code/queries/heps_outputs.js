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

const column_alters = () =>
  `
  ALTER COLUMN doi              SET OPTIONS(description="Full link to the Digitial Object Identifier of the paper"),
  ALTER COLUMN inst             SET OPTIONS(description="The code for this work's HEP"),
  ALTER COLUMN assignment       SET OPTIONS(description="Assignment method"),
  ALTER COLUMN classification   SET OPTIONS(description="Which field this work belongs to (for/foe)"),
  ALTER COLUMN openalex_id      SET OPTIONS(description="The OpenAlex identifier"),
  ALTER COLUMN journal_era_id   SET OPTIONS(description="ERA Journal List ID of the encompassing journal"),
  ALTER COLUMN journal_title    SET OPTIONS(description="Journal Title (from the ERA Journal List"),
  ALTER COLUMN paper_title      SET OPTIONS(description="Journal article title obtained from OpenAlex"),
  ALTER COLUMN oa               SET OPTIONS(description="Whether this work is open access"),
  ALTER COLUMN institutions     SET OPTIONS(description="Institutions that collaborated on this work"),
  ALTER COLUMN authors          SET OPTIONS(description="All authors obtained from OpenAlex"),
  ALTER COLUMN inst_authors     SET OPTIONS(description="All HEP authors obtained from OpenAlex"),
  ALTER COLUMN year             SET OPTIONS(description="Year of publication"),
  ALTER COLUMN citations        SET OPTIONS(description="Current number of citations"),
  ALTER COLUMN apportionment    SET OPTIONS(description="Research topic codes assigned to the paper with portions/weights. Each set of codes should sum to 100 portions at the 2-digit and 4-digit levels"),
  ALTER COLUMN hep_ror          SET OPTIONS(description="The ROR for this work's HEP"),
  ALTER COLUMN int_collab       SET OPTIONS(description="Whether this work is part of an international collaboration");
  `;

function compile_automatic({
  project = "",
  dataset = "",
  field = "",
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

  -- extract oa status, institution and country details
  doi_all AS (
    SELECT
     doi            AS doi,
     coki.oa.coki.open AS oa,
      ARRAY_AGG(STRUCT(
        institutions.identifier AS ror,
        institutions.types AS type,
        institutions.name AS name,
        institutions.country AS country_name,
        institutions.country_code_2 AS country_code
        )) AS institutions
      FROM ${doi_table}
  LEFT JOIN UNNEST(affiliations.institutions) AS institutions
  GROUP BY doi, oa
  ),

  assignments AS (
    SELECT 
      paper AS doi,
      inst AS ror,
      ARRAY_AGG(STRUCT(
        REPLACE(assig.field, "MD", "99") AS code,
        assig.frac                 AS weight,
        ROUND(rci.rci_world, 3)    AS rci_global
      )) AS apportionment
    FROM \`${project}.${dataset}.core_assignments${version}\` AS assig
    LEFT JOIN \`${project}.${dataset}.rci_papers${version}\` AS rci ON assig.paper = rci.doi and assig.field = rci.field
    GROUP BY doi, ror
  ),
  
  -- extract the HEP outputs identified by the RIES automatic by-line approach
  outputs AS (
  SELECT
    CONCAT('https://doi.org/', ries.doi) AS doi,
    all_heps.code                        AS inst, 
    '${assignment}'                      AS assignment,
    '${field}'                           AS classification,
    alex.id                              AS openalex_id,
    ries.era_id                          AS journal_era_id,
    alex.journal_title                   AS journal_title,
    alex.paper_title                     AS paper_title,
    doi_all.oa                           AS oa,
    doi_all.institutions                 AS institutions,
    alex.authors                         AS authors,
    alex.hep_authors                     AS inst_authors, #hep_authors,
    ries.year                            AS year,
    ries.citations                       AS citations,
    assignments.apportionment            AS apportionment,
    ries.institution                     AS hep_ror
  FROM \`${project}.${dataset}.heps_papers${version}\` AS ries
  LEFT JOIN alex ON ries.doi = alex.doi AND alex.hep_ror = ries.institution
  LEFT JOIN assignments ON ries.doi = assignments.doi AND ries.institution = assignments.ror
  LEFT JOIN \`${project}.${dataset}.heps${version}\` AS all_heps ON all_heps.ror = ries.institution
  LEFT JOIN doi_all ON ries.doi = doi_all.doi
  WHERE year >= ${start} AND year <= ${finish} 
  ORDER BY year, doi
),

  -- flag for international collaboration (all dois are asisgned to at least 1 AU HEP)
  int_collab AS (
    SELECT 
      doi,
      CASE WHEN COUNT(DISTINCT(institutions.country_code)) > 1 THEN TRUE ELSE FALSE END AS int_collab,
    FROM outputs,
    UNNEST(institutions) AS institutions
    GROUP BY doi
)

  -- final outputs table
  SELECT 
    outputs.*, 
    int_collab.int_collab
  FROM outputs 
  LEFT JOIN int_collab ON outputs.doi = int_collab.doi 
);


  ALTER TABLE \`${project}.${dataset}.heps_outputs${version}\` SET OPTIONS(description='HEP outputs for fields of education identified by the RIES by-line approach.');
  ALTER TABLE \`${project}.${dataset}.heps_outputs${version}\`
  ${column_alters()}

END;
`;
}

function compile_institutional({
  project = "",
  dataset = "",
  field = "",
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

  -- extract oa status, institution and country details
  doi_all AS (
    SELECT
     doi            AS doi,
     coki.oa.coki.open AS oa,
      ARRAY_AGG(STRUCT(
        institutions.identifier AS ror,
        institutions.types AS type,
        institutions.name AS name,
        institutions.country AS country_name,
        institutions.country_code_2 AS country_code
        )) AS institutions
      FROM ${doi_table}
  LEFT JOIN UNNEST(affiliations.institutions) AS institutions
  GROUP BY doi, oa
  ),

  assignments AS (
    SELECT 
      paper AS doi,
      inst AS ror,
      ARRAY_AGG(STRUCT(
        REPLACE(assig.field, "MD", "99") AS code,
        assig.frac                 AS weight,
        ROUND(rci.rci_world, 3)    AS rci_global
      )) AS apportionment
    FROM \`${project}.${dataset}.core_assignments${version}\` WHERE inst = '${hep_ror}' AS assig
    LEFT KOIN \`${project}.${dataset}.rci_papers${version}\` AS rci ON assig.paper = rci.doi and assig.field = rci.field
    GROUP BY doi, ror
  ),

  -- extract the HEP outputs identified by the RIES automatic by-line approach
  outputs AS (
  SELECT
    CONCAT('https://doi.org/', ries.doi) AS doi,
    all_heps.code                        AS inst, 
    '${assignment}'                      AS assignment,
    '${field}'                           AS classification,
    alex.id                              AS openalex_id,
    ries.era_id                          AS journal_era_id,
    alex.journal_title                   AS journal_title,
    alex.paper_title                     AS paper_title,
    doi_all.oa                           AS oa,
    doi_all.institutions                 AS institutions,
    alex.authors                         AS authors,
    alex.hep_authors                     AS inst_authors, #hep_authors,
    ries.year                            AS year,
    ries.citations                       AS citations,
    assignments.apportionment            AS apportionment,
    ries.institution                     AS hep_ror
  FROM \`${project}.${dataset}.heps_papers${version}\` AS ries
  LEFT JOIN alex ON ries.doi = alex.doi AND alex.hep_ror = ries.institution
  LEFT JOIN assignments ON ries.doi = assignments.doi AND ries.institution = assignments.ror
  LEFT JOIN \`${project}.${dataset}.heps${version}\` AS all_heps ON all_heps.ror = ries.institution
  LEFT JOIN doi_all ON ries.doi = doi_all.doi
  WHERE year >= ${start} AND year <= ${finish} AND institution = '${hep_ror}'
  ORDER BY year, doi
),

  -- flag for international collaboration (all dois are asisgned to at least 1 AU HEP)
  int_collab AS (
    SELECT 
      doi,
      CASE WHEN COUNT(DISTINCT(institutions.country_code)) > 1 THEN TRUE ELSE FALSE END AS int_collab,
    FROM outputs,
    UNNEST(institutions) AS institutions
    GROUP BY doi
)

  -- final outputs table
  SELECT 
    outputs.*, 
    int_collab.int_collab
  FROM outputs 
  LEFT JOIN int_collab ON outputs.doi = int_collab.doi 
  WHERE ries.institution = '${hep_ror}'
);

  ALTER TABLE \`${project}.${dataset}.heps_outputs${version}\` SET OPTIONS(description='HEP outputs for fields of education identified by the RIES by-line approach.');
  ALTER TABLE \`${project}.${dataset}.heps_outputs${version}\`
  ${column_alters()}
END;
`;
}

function compile(
  conf = {
    project: "",
    dataset: "",
    field: "",
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

function wrap_logs(s) {
  console.log(`heps_outputs.js :: ${s}`);
}

if (require.main === module) {
  fs = require("fs");
  const { path_config } = require("../utilities/path_manager.js");
  const config = require("../app.js").conf();

  let outfile = "data/heps_outputs_automatic_fors.sql";
  fs.writeFileSync(
    outfile,
    compile({
      ...config,
      dataset: "ries_fors",
      assignment: "automatic",
      field: "fors",
      doi_table: path_config.doi_table,
    }),
  );
  wrap_logs(`Automatic FORs query written to ${outfile}`);
  outfile = "data/heps_outputs_automatic_foes.sql";
  fs.writeFileSync(
    outfile,
    compile({
      ...config,
      dataset: "ries_foes",
      assignment: "automatic",
      field: "foes",
      doi_table: path_config.doi_table,
    }),
  );
  wrap_logs(`Automatic FOEs query written to ${outfile}`);

  config.institutional_hep_codes.forEach((hep_code) => {
    outfile = `data/heps_outputs_institutional_fors_${hep_code}.sql`;
    fs.writeFileSync(
      outfile,
      compile({
        ...config,
        dataset: `ries_fors_${hep_code}`,
        hep_code: hep_code,
        assignment: "institutional",
        field: "fors",
        doi_table: path_config.doi_table,
      }),
    );
    wrap_logs(`${hep_code} FORs query written to ${outfile}`);

    outfile = `data/heps_outputs_institutional_foes_${hep_code}.sql`;
    fs.writeFileSync(
      outfile,
      compile({
        ...config,
        dataset: `ries_foes_${hep_code}`,
        hep_code: hep_code,
        assignment: "institutional",
        field: "foes",
        doi_table: path_config.doi_table,
      }),
    );
    wrap_logs(`${hep_code} FOEs query written to ${outfile}`);
  });
}
