// RIES tasks.
// Each task takes the RIES config as an input
// Tasks will return {success: true}. If an error is raised, it will be thrown by utilities.checks.check_result

const fs = require("fs");

const { push } = require("./libraries/lib_gcs.js");
const { run_query, run_queries } = require("./utilities/run_queries.js");
const { path_config } = require("./utilities/path_manager.js");
const compile_ries = require("./utilities/compile_ries.js");
const compile_export_ries_tables = require("./queries/export_dashboard.js");
const { compile_hep_institutional } = require("./queries/export_hep_tables.js");
const output_hep_data = require("./queries/output_hep_data.js");
const { check_result, check_string } = require("./libraries/lib_checks.js");
const { connect } = require("./libraries/lib_bigquery.js");
const { SQLBatchWriter } = require("./utilities/json_to_sql.js");

function compile_ries_queries(config) {
  // Compiles the queries that create the main RIES tables
  console.log("Creating queries for database builing...");
  fs.writeFileSync(
    path_config.ries_query.auto.fors,
    compile_ries({
      ...config,
      dataset: "ries_fors",
      assignment: "automatic",
      field: "fors",
      doi_table: path_config.doi_table,
    }).join(""),
  );
  fs.writeFileSync(
    path_config.ries_query.auto.foes,
    compile_ries({
      ...config,
      dataset: "ries_foes",
      assignment: "automatic",
      field: "foes",
      doi_table: path_config.doi_table,
    }).join(""),
  );
  config.institutional_hep_codes.forEach((hep_code) => {
    fs.writeFileSync(
      path_config.ries_query.inst[hep_code].fors,
      compile_ries({
        ...config,
        dataset: `ries_fors_${hep_code}`,
        hep_code: hep_code,
        assignment: "institutional",
        field: "fors",
        doi_table: path_config.doi_table,
      }).join(""),
    );
    fs.writeFileSync(
      path_config.ries_query.inst[hep_code].foes,
      compile_ries({
        ...config,
        dataset: `ries_foes_${hep_code}`,
        hep_code: hep_code,
        assignment: "institutional",
        field: "foes",
        doi_table: path_config.doi_table,
      }).join(""),
    );
  });
  return { success: true };
}

function run_ries_queries(config) {
  // Runs the queries that create the main RIES tables
  let query_file = path_config.ries_query.auto.fors;
  console.log(`Running all queries in query file: ${query_file}`);
  check_result(run_queries(config.project, query_file));
  query_file = path_config.ries_query.auto.foes;
  console.log(`Running all queries in query file: ${query_file}`);
  check_result(run_queries(config.project, query_file));
  for (const hep_code of config.institutional_hep_codes) {
    query_file = path_config.ries_query.inst[hep_code].fors;
    console.log(`Running all queries in query file: ${query_file}`);
    check_result(run_queries(config.project, query_file));
    query_file = path_config.ries_query.inst[hep_code].foes;
    console.log(`Running all queries in query file: ${query_file}`);
    check_result(run_queries(config.project, query_file));
  }
  return { success: true };
}
function export_ries_tables(config) {
  // Exports the ries tables to bigquery tables
  const exports = [
    {
      dataset: "ries_foes",
      filename: path_config.ries_table.auto.filename.foes,
      query: path_config.ries_table.auto.query.foes,
    },
    {
      dataset: "ries_fors",
      filename: path_config.ries_table.auto.filename.fors,
      query: path_config.ries_table.auto.query.fors,
    },
    ...config.institutional_hep_codes.flatMap((hep_code) => [
      {
        dataset: `ries_foes_${hep_code}`,
        filename: path_config.ries_table.inst.filename[hep_code].foes,
        query: path_config.ries_table.inst.query[hep_code].foes,
      },
      {
        dataset: `ries_fors_${hep_code}`,
        filename: path_config.ries_table.inst.filename[hep_code].fors,
        query: path_config.ries_table.inst.query[hep_code].fors,
      },
    ]),
  ];
  for (const params of exports) {
    console.log(
      `Running GUI table export for ${params.dataset} -> ${params.filename}`,
    );
    let result = run_query(
      config.project,
      compile_export_ries_tables({
        ...config,
        dataset: params.dataset,
        filename: params.filename,
      }),
      params.query,
    );
    check_result(result);
  }
  return { success: true };
}

function run_output_queries(config) {
  // Creates the tables for a HEP in BQ using institutional assignment
  // Tables are stored in their institution's project: ries-{HEP} and the config.project
  for (const hep_code of config.institutional_hep_codes) {
    console.log(
      `Creating 'output' tables for hep (institutional): ${hep_code}`,
    );
    let output_project = `ries-${hep_code.toLowerCase()}`;
    if (config.inst_project_override) {
      output_project = config.inst_project_override;
      console.log(
        `Overwriting institutional output project: ${output_project}`,
      );
      check_string(output_project);
    } else {
      console.log(`Writing output to project: ${output_project}`);
    }

    let output_projects = [output_project, config.project];
    if (output_project === config.project) {
      // Don't double write the table
      output_projects = output_projects[0];
    }

    for (out of output_project) {
      let result = run_query(
        config.project,
        compile_hep_institutional({
          project: config.project,
          output_project: out,
          version: config.version,
          doi_table: path_config.doi_table,
          hep_code: hep_code,
        }),
        path_config.create_output[hep_code],
      );
      check_result(result);
      console.log(
        `Output institutional/automatic datasets written to: ${output_project}`,
      );
    }
  }
  return { success: true };
}

async function export_hep_outputs(config) {
  // Export the hep assignments to a .sql file for the D1 database.
  // This is done by streaming the query results through the processing
  // function and into a file. Which is then uploaded to a bucket.
  async function process_query_stream(stream, handler) {
    for await (const row of stream) {
      await handler.handle_input(row);
    }
  }

  const bq_link = connect({
    keyfile: config.keyfile,
    project: config.project,
  });

  // Handler to write to the stream
  const query_writer = new SQLBatchWriter({
    dir: "data",
    file_pre: path_config.export_output.sql_file,
    max_rows_per_insert: 10,
    max_inserts_per_file: 5000,
  });

  console.log("Exporting automatic HEP outputs");

  let sql = output_hep_data.compile({
    for_table: `${config.project}.ries_fors.heps_outputs${config.version}`,
    foe_table: `${config.project}.ries_foes.heps_outputs${config.version}`,
  });
  fs.writeFileSync(path_config.export_output.auto_query, sql);
  await process_query_stream(bq_link.bq.createQueryStream(sql), query_writer);

  for (hep of config.institutional_hep_codes) {
    console.log(`Exporting institutional HEP outputs: ${hep}`);
    sql = output_hep_data.compile({
      for_table: `${config.project}.ries_fors_${hep}_institutional_outputs.heps_outputs${config.version}`,
      foe_table: `${config.project}.ries_foes_${hep}_institutional_outputs.heps_outputs${config.version}`,
    });
    await process_query_stream(bq_link.bq.createQueryStream(sql), query_writer);
    fs.writeFileSync(path_config.export_output.inst_queries[hep], sql);
  }
  await query_writer.handle_close(); // Close the stream

  console.log("Uploading results to cloud bucket...");
  let uri, fname;
  for (i = 0; i < query_writer.files.length; i++) {
    fname = query_writer.files[i];
    uri = `${path_config.export_output.sql_uri}${i + 1}.sql`;
    result = push(query_writer.files[i], uri);
    result.success = result.code === 0;
    check_result(result);
    console.log(`Uploaded ${fname} to: ${uri}`);
  }
}
const tasks = {
  compile_ries_queries,
  run_ries_queries,
  export_ries_tables,
  run_output_queries,
  export_hep_outputs,
};
module.exports = tasks;
