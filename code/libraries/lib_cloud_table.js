/*
## Description
Abstraction of a data table with functions that make it easy to move between BigQuery, GCS and local disk

- uses GCloud CLI tools (bq and gsutil)
- ensure that you are signed into the CLI with a user that has sufficient privileges

$account MAY be your GCloud account. If unspecified, the current CLI value is used.

  YES -> account = "" // defaults to the current CLI setting
  YES -> account = "my.name@email.address.com"
  NO  -> account = "my.name"

$project MAY be your GCloud project. If unspecified, the current CLI value is used.

  YES -> project = "" // defaults to the current CLI setting
  YES -> project = "project"

$table MUST contain the dataset and MAY contain the project

  YES -> table = "project:dataset.table"
  YES -> table = "dataset.table"
  NO  -> table = "table"

$cloud MUST be a fully qualified GCS path that MAY contain acceptable wildcards

  YES -> cloud = "gs://bucket/path/file.ext"
  YES -> cloud = "gs://bucket/path/files*.ext"
  NO  -> cloud = "/bucket/path/file.ext"

$local MUST be a fully qualified local filesystem path to an existing directory

  YES -> local = "/absolute/path/to/directory"
  YES -> local = "/absolute/path/to/directory/"
  NO  -> local = "../relative/path/to/directory"

## Contacts
julian.tonti-filippini@curtin.edu.au

## License
Apache 2.0
*/
const exec = require("./lib_exec");
const GCloud = require("./lib_gcloud");

class CloudTable {
  constructor({ project, dataset, name, base_cloud, base_local }) {
    // this.account    = account || 'account_name';
    this.project = project || "project_name";
    this.dataset = dataset || "dataset_name";
    this.name = name || "table_name";
    this.base_cloud = base_cloud || "gs://bucket/path";
    this.base_local = base_local || "/tmp/path";
    this.local = `${this.base_local}/${this.name}`;
    this.cloud = `${this.base_cloud}/${this.name}`;
    this.query = "SELECT * FROM `some.table`";
    this.schema = "";
    this.verbose = false;
    this.dryrun = false;
    this.gcloud = new GCloud();
    this.gcloud.account = this.account;
    this.gcloud.project = this.project;
    this.authenticate(this);
  }
  exec(s) {
    return exec(s, this);
  }
  authenticate({ account = "", project = "" }) {
    if (account) this.exec(`gcloud config set account "${account}"`);
    if (project) this.exec(`gcloud config set project "${project}"`);
  }

  // source of truth is a Google BigQuery SQL query
  query_to_table() {
    this.exec(
      `bq query --use_legacy_sql=false 'CREATE OR REPLACE TABLE \`${this.name}\` AS (${query.replace(/(?<!\\)'/g, "\\'")});'`,
    );
    if (this.schema) {
      this.exec(`bq update --table --schema "${this.schema}" "${this.name}"`);
    } else {
      this.schema = this.exec(`bq show --format='json' "${this.name}"`);
    }
  }
  query_to_cloud() {
    this.query_to_table(this.query);
    this.table_to_cloud();
    this.table_delete();
  }
  query_to_local() {
    this.query_to_cloud(this.query);
    this.cloud_to_local();
    this.cloud_delete();
  }

  // source of truth is a Google BigQuery table
  table_to_cloud() {
    this.cloud_delete();
    this.exec(
      `bq show --format='json' "${this.name}" | gsutil cp - "${this.cloud}/schema.json"`,
    );
    this.exec(
      `bq extract --destination_format=NEWLINE_DELIMITED_JSON "${this.name}" "${this.cloud}/data.*.jsonl"`,
    );
  }
  table_to_local() {
    this.table_to_cloud();
    this.cloud_to_local();
    this.cloud_delete();
  }
  table_delete() {
    if (this.table_exists()) this.exec(`bq rm --table "${this.name}"`);
  }
  table_exists() {
    return this.exec(`bq show "${this.name}"`)[0] === null;
  }

  // source of truth is a Google Cloud Storage folder
  cloud_to_table() {
    this.table_delete();
    this.schema = this.exec(`gsutil cat "${this.cloud}/schema.json"`);
    this.exec(`bq mk --table --schema "${this.schema}" "${this.name}"`);
    this.exec(
      `bq load --source_format=NEWLINE_DELIMITED_JSON "${this.name}" "${this.cloud}/data*"`,
    );
  }
  cloud_to_local() {
    this.local_delete();
    this.exec(`gsutil -m cp "${this.cloud}" "${this.local}"`);
  }
  cloud_delete() {
    if (this.cloud_exists()) this.exec(`gsutil -m rm "${this.cloud}/*"`);
  }
  cloud_exists() {
    return this.exec(`gsutil ls ${this.cloud}/data*.jsonl`)[0] == null;
  }

  // source of truth is a local folder
  local_to_cloud() {
    //this.cloud_delete();
    console.log(`gsutil -m cp "${this.local}/data*" "${this.cloud}/"`);
    this.exec(`gsutil -m cp "${this.local}/data*" "${this.cloud}/"`);
    //this.exec(`gsutil -m cp "${this.local}/data*" "${this.cloud}/"`);
  }
  local_to_table() {
    this.local_to_cloud();
    this.cloud_to_table();
    this.cloud_delete();
  }
  local_delete() {
    if (this.local_exists()) this.exec(`rm -r "${this.local}"`);
  }
  local_exists() {
    return this.exec(`ls ${this.local}/data*.jsonl`)[0] == null;
  }

  // use with caution, this will eliminate the BQ table, local folder and cloud folder
  elmininate() {
    this.cloud_delete();
    this.table_delete();
    this.local_delete();
  }
}
module.exports = CloudTable;
