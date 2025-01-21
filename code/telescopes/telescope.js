/*
## Summary
Base telescope that other telescopes can mix in

## Contacts
julian.tonti-filippini@curtin.edu.au

## License
Apache 2.0

*/
const app = require("../app");
const CloudTable = require("../libraries/lib_cloud_table");

class Telescope {
  constructor({
    project = "",
    verbose = true,
    replace = false,
    compress = false,
    name = "",
    schema = null,
    keyfile = "",
  }) {
    if (!project) app.die("GCloud project must be specified");
    if (!name) app.die("Table name must be specified");
    if (!schema) app.die("Table schema must be specified");
    if (!keyfile) app.die("Keyfile must be specified");
    this.project = project;
    this.bucket = `gs://${project}-ries`;
    this.verbose = verbose;
    this.replace = replace;
    this.compress = compress;
    this.name = name;
    this.schema = schema;
    this.base_local = app.reserve(__dirname, ".data");
    app.reserve(this.base_local, this.name);
    this.base_cloud = `${this.bucket}/tables`;
    this.cloud_table = new CloudTable(this);
    process.env.GOOGLE_APPLICATION_CREDENTIALS = keyfile;
  }
  // use with caution, this will delete the BigQuery table, delete the cloud folder and delete the local folder
  eliminate() {
    this.cloud_table.eliminate();
  }
  log(...s) {
    if (this.verbose) app.log(...s);
  }
  path_local(f) {
    return `${this.base_local}/${this.name}/${f || ""}`;
  }
  path_cloud(f) {
    return `${this.base_cloud}/${this.name}/${f || ""}`;
  }
  save(d, f) {
    return this.replace || !app.file.is_file(this.path_local(f))
      ? app.save(d, this.path_local(f))
      : false;
  }
  load(f) {
    return app.load(this.path_local(f));
  }

  // uses cURL to download a file to the local folder, You must specify the destination file name
  async download(url = "", file = "") {
    this.log("Download:");
    this.log(`  Source: ${url}`);
    this.log(`  Target: ${this.path_local(file)}`);
    if (app.file.is_file(this.path_local(file))) {
      this.log(`  Skipped (target already exists)`);
    } else {
      app.curl(url, this.path_local(file));
      this.log(`  Download complete`);
    }
  }

  // transform input file(s) to data.jsonl
  async transform(transformer, ifile, ofile = "data.jsonl") {
    this.log("Transform:");
    this.log(`  Source: ${this.path_local(ifile)}`);
    this.log(`  Target: ${this.path_local(ofile)}`);
    if (this.cloud_table.local_exists()) {
      this.log(`  Skipped (target already exists)`);
    } else {
      await transformer(this, ifile, ofile);
      this.log(`  Transformation complete`);
    }
  }

  // copies schema.json and data*.jsonl files to google cloud storage
  async upload(file = "data.jsonl") {
    this.log(`Upload:`);
    this.log(`  Source: ${this.path_local(file)}`);
    this.log(`  Target: ${this.path_cloud(file)}`);
    if (this.cloud_table.cloud_exists()) {
      this.log(`  Skipped (target already exists)`);
    } else {
      this.cloud_table.local_to_cloud();
      this.log(`  Upload complete`);
    }
  }

  // create the BigQuery table
  async load_data(file = "data.jsonl") {
    this.log("Importing");
    this.log(`  Source: ${this.path_cloud(file)}`);
    this.log(`  Target: ${this.path_table(file)}`);
    if (this.cloud_table.table_exists()) {
      this.log(`  Skipped (target already exists)`);
    } else {
      this.cloud_table.cloud_to_table();
      this.log(`  Import complete`);
    }
  }
}
module.exports = Telescope;

if (require.main === module) {
  const args = app.conf();
  const scope = new Telescope(args);
  console.log(scope);
}
