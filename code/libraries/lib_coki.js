/*
## Description
Some utilities for providing COKI specific functionality. Intended to work with a config.js file.

## Contacts
julian.tonti-filippini@curtin.edu.au

## License
Apache 2.0
*/
const db = require('./lib_bigquery');
const coki = {
  keyfile : '', // path to the keyfile that allows connection to the COKI database
  project : '', // bigquery project ID
  dataset : '', // bigquery dataset ID
  table   : '', // doi table name and version
  ns      : '', // fully qualified bigquery namepace. This will be populated automatically from the other params when configured

  async connect({keyfile,project,dataset,table=''}) {
    try {
      coki.keyfile = keyfile;
      coki.project = db.safe_name(project);
      coki.dataset = db.safe_name(dataset);
      coki.table   = db.safe_name(table);
      coki.link    = db.connect({
        keyfile : coki.keyfile,
        project : coki.project,
        dataset : coki.dataset,
      });
      if (coki.table === 'latest' || coki.table === '') {
        coki.table = await coki.get_latest_doi_table();
      }
      coki.ns = `${coki.project}.${coki.dataset}.${coki.table}`;
    }
    catch (e) {
      console.error(`ERROR: unable to connect to COKI database using:`, { keyfile, project, dataset, table });
      console.error(e);
      process.exit(1);
    }
  },
  async list_doi_tables() {
    return (await db.list_tables(coki.link)).filter(v => (v + '').startsWith('doi')).map(v => db.safe_name(v)).filter(v => v).sort();
  },
  async get_latest_doi_table() {
    return (await coki.list_doi_tables()).at(-1);
  },
  // run a query using the COKI connection
  async query(sql) {
    return await db.query(coki.link,sql);
  }
};
module.exports = coki;

if (require.main === module) {
  async function test() {
    const args = require('app').conf();
    await coki.connect({
      keyfile : args.keyfile,
      project : args.coki_project,
      dataset : args.coki_dataset,
      table   : 'latest',
    });
    const tables = await coki.list_doi_tables();
    const latest = tables.at(-1);
    const namespace = `${coki.project}.${coki.dataset}.${latest}`;
    console.log('Available:', tables);
    console.log('Latest:', latest);
    console.log('Namespace:', namespace);
  }
  test();
}
