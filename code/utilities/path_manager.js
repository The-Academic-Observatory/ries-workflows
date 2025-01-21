// For keeping track of paths, uris etc.
const path = require("path");
const conf = require("../app").conf();

const prepend = (fname) => path.join("data", fname);

const path_config = {
  // RIES compilation
  ries_query: ries_queries(conf),
  // RIES table creation
  ries_table: ries_table(conf),
  // HEP table creation
  create_output: create_output(conf),
  // HEP table export
  export_output: export_output(conf),
  // COKI DOI Table
  doi_table: `${conf.coki_project}.${conf.coki_dataset}.doi${conf.doi_table_version}`,
};

function ries_queries(config) {
  let inst_files = Object.fromEntries(
    config.institutional_hep_codes.map((hep_codes) => [
      hep_codes,
      {
        fors: prepend(
          `.compile_ries_fors_institutional_${hep_codes}_${config.version}.sql`,
        ),
        foes: prepend(
          `.compile_ries_foes_institutional_${hep_codes}_${config.version}.sql`,
        ),
      },
    ]),
  );
  return {
    auto: {
      fors: prepend(`.compile_ries_fors_${config.version}.sql`),
      foes: prepend(`.compile_ries_foes_${config.version}.sql`),
    },
    inst: inst_files,
  };
}

function ries_table(config) {
  // The file names and queries for both 'automatic' assignment and 'institutional'
  // Institutional is per-HEP
  const auto_filenames = {
    fors: `automatic_fors_${config.version}`,
    foes: `automatic_foes_${config.version}`,
  };
  const inst_filenames = Object.fromEntries(
    config.institutional_hep_codes.map((hep_code) => [
      hep_code,
      {
        fors: `institution_fors_${hep_code}_${config.version}`,
        foes: `institution_foes_${hep_code}_${config.version}`,
      },
    ]),
  );
  const inst_queries = Object.fromEntries(
    config.institutional_hep_codes.map((code) => [
      code,
      {
        fors: prepend(`.export_gui_${inst_filenames[code].fors}.sql`),
        foes: prepend(`.export_gui_${inst_filenames[code].foes}.sql`),
      },
    ]),
  );

  return {
    auto: {
      filename: auto_filenames,
      query: {
        fors: prepend(`.export_gui_${auto_filenames.fors}.sql`),
        foes: prepend(`.export_gui_${auto_filenames.foes}.sql`),
      },
    },
    inst: { filename: inst_filenames, query: inst_queries },
  };
}

function create_output(config) {
  // Query names for output
  return Object.fromEntries(
    config.institutional_hep_codes.map((hep_code) => [
      hep_code,
      prepend(`.export_output_${hep_code}_institutional_${config.version}.sql`),
    ]),
  );
}

function export_output(config) {
  // Queries and uri for hep outputs
  const inst_queries = Object.fromEntries(
    config.institutional_hep_codes.map((hep_code) => [
      hep_code,
      prepend(`.outputs_institutional_${hep_code}_${config.version}.sql`),
    ]),
  );
  return {
    inst_queries: inst_queries,
    auto_query: prepend(`.outputs_automatic_${config.version}.sql`),
    sql_file: `.outputs_d1_${config.version}_`,
    sql_uri: `${config.project}-ries/tables/heps_outputs/${config.version}/outputs_all_`,
  };
}
module.exports = { path_config, prepend };

if (require.main === module) {
  console.log(JSON.stringify(path_config, null, 2));
}
