/*

1. generate SQL text files, produces .compile_ries_${version}_* files that are then copied into the BQ console and run to generate datasets and tables.
2. once the datasets are created in BQ, uncomment out export GUI tables to the cloud bucket lines and upload the gui files as csv to GCS, for use in the RIES app (save, run node all.js)
camel case '*_cc*' csv files are downloaded from GCS to the local data folder in the RIES app and uploaded to cloudflareR2 bucket

*/
const fs = require("fs");
const { conf } = require("./code/app.js");
const tasks = require("./code/tasks.js");
const { path_config } = require("./code/utilities/path_manager.js");

async function run(run_config) {
  // Runs the RIES workflow.
  // Will check the config for each 'task'. If the task is enabled, it will be run with the supplied config.
  if (!run_config.version) {
    console.log(
      `Version string empty, will use DOI table version: ${run_config.doi_table_version}`,
    );
    run_config.version = run_config.doi_table_version;
  }
  // Output the path config to a file for debuging purposes
  fs.writeFileSync(".path_config", JSON.stringify(path_config, null, 2));
  console.log(`Path config written to '.path_config' for debugging`);

  console.log("Running with config:");
  console.log(run_config);
  let todo = [
    // 1. Compile standard and HEP queries to files
    run_config.compile_ries_queries
      ? tasks.compile_ries_queries
      : tasks.compile_ries_queries.name,
    // 2. Run the queries - Creates the main RIES tables
    run_config.run_ries_queries
      ? tasks.run_ries_queries
      : tasks.run_ries_queries.name,
    // 3. Export GUI tables to the cloud bucket
    run_config.export_ries_tables
      ? tasks.export_ries_tables
      : tasks.export_ries_tables.name,
    // 4. Make the 'output' tables
    run_config.run_output_queries
      ? tasks.run_output_queries
      : tasks.run_output_queries.name,
    // 5. Export 'output' tables to .sql file
    run_config.export_hep_outputs
      ? tasks.export_hep_outputs
      : tasks.export_hep_outputs.name,
  ];
  for (const fn of todo) {
    if (typeof fn === "function") {
      console.log(`Running task: ${fn.name}`);
      try {
        await Promise.resolve(fn(run_config));
      } catch (error) {
        console.error(`Error occurred in task ${fn.name}:\n ${error.stack}`);
        return;
      }
    } else {
      console.log(`Skipping task: ${fn}`);
    }
  }

  console.log("Done!!");
}

if (require.main === module) {
  run(conf());
}
