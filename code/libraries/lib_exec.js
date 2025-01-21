/*
## Description
Run BASH commands. Wraps execSync with some helpers.

## Contacts
julian.tonti-filippini@curtin.edu.au

## License
Apache 2.0
*/

const { execSync } = require("node:child_process");

function exec(cmd = "", args = {}) {
  return exec_ext(cmd, args);
}
function exec_ext(
  cmd = "",
  { verbose = false, dryrun = false, format = "text", eformat = "text" },
) {
  if (verbose) console.log(cmd);
  if (dryrun) return [null, null];
  let result = null;
  let error = null;
  try {
    result = parse(execSync(cmd, { stdio: "pipe" }).toString(), format);
  } catch (e) {
    error = parse(e.stderr.toString(), eformat);
  }
  if (error) {
    console.error(error);
  } else {
    console.log(result);
  }
  return [error, result];
}
function parse(str = "", format = "") {
  if (format == "text") return str.trim();
  if (format == "json") return JSON.parse(str);
  if (format == "jsonl")
    return str
      .trim()
      .split("\n")
      .map((line) => JSON.parse(line));
  if (format == "lines") return str.trim().split("\n");
  return str;
}
module.exports = exec;

if (require.main == module && process.argv[2] && !process.argv[3]) {
  console.log(exec(process.argv[2], { verbose: true, dryrun: true }));
  console.log(exec(process.argv[2]));
  console.log(exec(process.argv[2], { format: "lines" }));
}
