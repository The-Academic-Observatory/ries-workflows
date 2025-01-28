/*

There are four sets of tables:

- standard RIES with FoEs   (dataset: project.ries_YYYYMMDD_foes_outputs.*)
- standard RIES with FoRs   (dataset: project.ries_YYYYMMDD_fors_outputs.*)
- HEP-assignments with FoEs (dataset: project.ries_YYYYMMDD_foes_hep_outputs.*)
- HEP-assignments with FoRs (dataset: project.ries_YYYYMMDD_fors_hep_outputs.*)

The tables that are exported are:

- heps
- heps_class_tallies_by_field_year_2
- heps_class_tallies_by_field_year_4
- heps_gui_table
- heps_paper_classes_2
- heps_paper_classes_4
- heps_papers
- heps_summary_by_field_year_2
- heps_summary_by_field_year_4

# using the bq command line tool to interact with tables
src="project_id:dataset.source_table"
dest="project_id:dataset.destination_table"
meta="table_metadata.json"

# copy a table to a new table
bq cp $src $dest

# print the metadata for the source table to a local file
bq show --format=prettyjson $dest > $meta

# update the metadata for the destination table
bq update --source $meta $dest

# delete some records from the destination table
bq query --use_legacy_sql=false "DELETE FROM ${dest} WHERE some_condition"

*/
const { hep_map } = require("../libraries/lib_hep_map.js");
const { check_hep_ror, check_hep_code } = require("../libraries/lib_checks.js");

const compile = ({
  idataset = "",
  odataset = "",
  hep_code = "",
  version = "",
}) => {
  check_hep_code(hep_code);
  hep_ror = hep_map[hep_code].ror;
  check_hep_ror(hep_ror);
  return `-- create dataset ${odataset} sourced from ${idataset}
CREATE SCHEMA IF NOT EXISTS \`${odataset}\`;
CREATE OR REPLACE TABLE \`${odataset}.heps_gui_table${version}\`                      AS (SELECT * FROM \`${idataset}.heps_gui_table${version}\`                      WHERE idx_hep = '${hep_code}');
CREATE OR REPLACE TABLE \`${odataset}.heps_class_tallies_by_field_year_2_${version}\` AS (SELECT * FROM \`${idataset}.heps_class_tallies_by_field_year_2_${version}\` WHERE institution = '${hep_ror}');
CREATE OR REPLACE TABLE \`${odataset}.heps_class_tallies_by_field_year_4_${version}\` AS (SELECT * FROM \`${idataset}.heps_class_tallies_by_field_year_4_${version}\` WHERE institution = '${hep_ror}');
CREATE OR REPLACE TABLE \`${odataset}.heps_paper_classes_2_${version}\`               AS (SELECT * FROM \`${idataset}.heps_paper_classes_2_${version}\`               WHERE institution = '${hep_ror}');
CREATE OR REPLACE TABLE \`${odataset}.heps_paper_classes_4_${version}\`               AS (SELECT * FROM \`${idataset}.heps_paper_classes_4_${version}\`               WHERE institution = '${hep_ror}');
CREATE OR REPLACE TABLE \`${odataset}.heps_papers${version}\`                         AS (SELECT * FROM \`${idataset}.heps_papers${version}\`                         WHERE institution = '${hep_ror}');
CREATE OR REPLACE TABLE \`${odataset}.heps_assignments${version}\`                    AS (SELECT * FROM \`${idataset}.core_assignments${version}\`                    WHERE inst        = '${hep_ror}');
CREATE OR REPLACE TABLE \`${odataset}.heps_summary_by_field_year_2_${version}\`       AS (SELECT * FROM \`${idataset}.heps_summary_by_field_year_2_${version}\`       WHERE institution = '${hep_ror}');
CREATE OR REPLACE TABLE \`${odataset}.heps_summary_by_field_year_4_${version}\`       AS (SELECT * FROM \`${idataset}.heps_summary_by_field_year_4_${version}\`       WHERE institution = '${hep_ror}');
CREATE OR REPLACE TABLE \`${odataset}.heps_outputs${version}\`                        AS (SELECT * FROM \`${idataset}.heps_outputs${version}\` WHERE hep_ror = '${hep_ror}');
`;
};

const compile_hep_institutional = ({
  project = "",
  output_project = "",
  version = "",
  hep_code = "",
  doi_table = "",
}) => {
  // For a HEP with institutional assignments, compiles the custom 'institutional' and 'automatic' datasets
  // Compiled datasets are written to the output project
  let sqls = [];
  sqls.push(
    compile({
      idataset: `${project}.ries_foes_${hep_code}`,
      odataset: `${output_project}.ries_foes_${hep_code}_institutional_outputs`,
      hep_code: hep_code,
      version: version,
      doi_table: doi_table,
    }),
  );
  sqls.push(
    compile({
      idataset: `${project}.ries_fors_${hep_code}`,
      odataset: `${output_project}.ries_fors_${hep_code}_institutional_outputs`,
      hep_code: hep_code,
      version: version,
      doi_table: doi_table,
    }),
  );
  sqls.push(
    compile({
      idataset: `${project}.ries_foes`,
      odataset: `${output_project}.ries_foes_${hep_code}_automatic_outputs`,
      hep_code: hep_code,
      version: version,
      doi_table: doi_table,
    }),
  );
  sqls.push(
    compile({
      idataset: `${project}.ries_fors`,
      odataset: `${output_project}.ries_fors_${hep_code}_automatic_outputs`,
      hep_code: hep_code,
      version: version,
      doi_table: doi_table,
    }),
  );
  return sqls.join("");
};

module.exports = { compile, compile_hep_institutional };
if (require.main === module) {
  config = require("../app").conf();
  console.log(config);
  config.institutional_hep_codes.forEach((hep_code) => {
    console.log(
      compile_hep_institutional({
        project: config.project,
        version: config.version,
        hep_code: hep_code,
        doi_table: config.doi_table,
      }),
    );
  });
}
