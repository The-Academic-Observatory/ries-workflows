/*

Given a generated SQL file, break it up into chunks and exec all the queries via the CLI.

*/
const { readFileSync, createWriteStream, writeFileSync } = require("fs");
const { prepend } = require("./path_manager");
const { exec, execSync } = require("node:child_process");

// async function run_query_stream_json(
//   project = "",
//   query = "",
//   qfile = null,
//     outfile = ""
// ) {
//
//   if (!qfile) {
//     qfile = prepend(`.query_${Date.now()}.sql`);
//   }
//     let current_write_stream = null
//     const createNewWriteStream = (basePath) => {
//         const parts = basePath.split('.');
//         const newPath = currentFileIndex === 1
//             ? basePath
//             : `${parts[0]}_${currentFileIndex}.${parts[1]}`;
//
//         current_write_stream = createWriteStream(newPath);
//         currentFileSize = 0;
//         return current_write_stream;
//     };
//   try {
//   }
//
// }

function run_query(project = "", query = "", qfile = null, format = "none") {
  if (!qfile) {
    qfile = prepend(`.query_${Date.now()}.sql`);
  }
  try {
    writeFileSync(qfile, query);
    const command = `bq query --use_legacy_sql=false --project_id='${project}' --format=${format} -n 100000000 --batch < ${qfile}`;
    const output = execSync(command, {
      stdio: "pipe",
      maxBuffer: 1024 * 1024 * 1024,
    }).toString(); // 1GB buffer
    return {
      success: true,
      message: "Query executed successfully",
      output: output,
    };
  } catch (error) {
    return {
      success: false,
      message: `Error executing query: ${error.message}`,
    };
  }
}

function run_queries(project = "", ifile = "", skip = 0) {
  console.log(`Processing file ${ifile}`);
  const queries = readFileSync(ifile).toString().trim().split("BEGIN");
  for (let i = 0 + skip, len = queries.length; i < len; ++i) {
    const query = (i > 0 ? "BEGIN" : "") + queries[i];
    console.log(`Query ${i + 1} of ${len}: ${query.substring(0, 40)}...`);
    const result = run_query(project, query);
    if (!result.success) {
      console.error(result.message);
      return result;
    }
  }
  return {
    success: true,
    message: "Queries executed successfully",
  };
}

function test() {
  console.log(
    run_query("", "SELECT table_name FROM ries.INFORMATION_SCHEMA.TABLES;"),
  );
  console.log(
    run_query("", "SELECT table_name FROM ries.INFORMATION_SCHEMA.TABLES2;"),
  );
}
module.exports = { run_query, run_queries };
