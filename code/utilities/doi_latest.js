/*
Get a list of all the available COKI doi tables
*/
async function list() {
  const coki = require("../libraries/lib_coki");
  const args = require("app").conf();
  await coki.connect({
    keyfile: args.keyfile,
    project: args.coki_project,
    dataset: args.coki_dataset,
    table: "latest",
  });
  const tables = await coki.list_doi_tables();
  const latest = tables.at(-1);
  const namespace = `${coki.project}.${coki.dataset}.${latest}`;
  return { tables, latest, namespace };
}
async function latest() {
  return (await list()).latest;
}
async function print() {
  const { tables, latest, namespace } = await list();
  console.log("Available:", tables);
  console.log("Latest:", latest);
  console.log("Namespace:", namespace);
}
module.exports = { list, latest, print };

if (require.main === module) {
  print();
}
