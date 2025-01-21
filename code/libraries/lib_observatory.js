/*
## Description
Utilities for interacting with the Academic Observatory

## Contacts
julian.tonti-filippini@curtin.edu.au

## License
Apache 2.0
*/
const bigquery = require('../../lib/lib_bigquery');
const link = bigquery.get_link('academic-observatory');

// tables in the observatory dataset are snapshotted once per week. The name of each table is 
// suffixed with the date of the snapshot. Returns a mapping of base table name to current name.
async function get_most_recent_tables() {
  let names = {};
  let dates = {};
  let tables = await bigquery.list_tables(link, 'observatory');
  for (let t of tables) {
    let name = t.substring(0,t.length-8);
    let date = t.substring(t.length-8);
    names[name] = true;
    dates[date] = true;
  }
  let most_recent_date = Object.keys(dates).map(v => +v).sort().pop();
  return  Object.fromEntries(Object.keys(names).sort().map(name => [name, `observatory.${name}${most_recent_date}`]));
}

if (require.main === module) {
  async function test() {
    let tables = await get_most_recent_tables();
    console.log(tables);
  }
  test();
}