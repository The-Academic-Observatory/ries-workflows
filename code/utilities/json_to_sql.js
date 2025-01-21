/*
Takes the output of a query from export_hep_tables.js as input
Converts the result to and .sql
*/

const fs = require("fs");
const path = require("path");

class SQLBatchWriter {
  // Class for dumping the query result from export_hep_tables.js to a series
  // of .sql INSERT INTO statements.
  // Does this by dumping max_rows_per_insert rows into an insert statement.
  // Will write max_inserts_per_file insert statements before begin writing to a new file.
  constructor({
    dir = "./",
    file_pre = "sql_dump_",
    max_rows_per_insert = 1,
    max_inserts_per_file = 250000,
  }) {
    this.dir = dir; // The directory to write to
    this.file_pre = file_pre; // The file prefix. Will be appended with {NUMBER}.sql
    this.max_rows_per_insert = max_rows_per_insert;
    this.max_inserts_per_file = max_inserts_per_file;

    // State
    this.files = [];
    this.current_rows = [];
    this.current_file_number = 0;
    this.current_insert_count = 0;
    this.ostream = null;
  }

  async handle_input(row) {
    // Decides what to do with a single row from the query stream
    if (!this.ostream) {
      await this.new_stream();
    }
    this.current_rows.push(row);

    if (this.current_rows.length === this.max_rows_per_insert) {
      await this.flush_rows();
    }

    if (this.current_insert_count === this.max_inserts_per_file) {
      await this.handle_close();
    }

    if (
      this.current_rows.length > this.max_rows_per_insert ||
      this.current_insert_count > this.max_inserts_per_file
    ) {
      console.log(`Error in stream writing`);
      console.log(`N rows in buffer: ${this.current_rows.length}`);
      console.log(`Instert count: ${this.current_insert_count}`);
      throw new Error();
    }
  }

  async handle_close() {
    await this.flush_rows(); // Flush the rows

    console.log("Closing write stream");
    await new Promise((resolve, reject) => {
      this.ostream.end(() => resolve());
      this.ostream.on("error", reject);
    });

    this.ostream = null;
  }

  async check_buffer() {
    // Checks that the buffer isn't full. If it is, writes it to stream
    if (this.ostream.writableLength > this.ostream.writableHighWaterMark) {
      await new Promise((resolve) => this.ostream.once("drain", resolve));
    }
  }

  async write_to_stream(row) {
    await new Promise((resolve, reject) => {
      const can_continue = this.ostream.write(row, (err) => {
        if (err) reject(err);
        if (!can_continue) resolve();
      });
      if (can_continue) resolve();
    });
  }

  async flush_rows() {
    // Dumps rows to stream
    if (!this.current_rows || this.current_rows.length === 0) {
      return;
    }
    const insert_into =
      `INSERT INTO outputs (inst, classification, assignment, doi, openalex_id, journal_era_id, journal_title, ` +
      `paper_title, inst_authors, authors, year, citations, apportionment) VALUES\n`;
    await this.write_to_stream(insert_into);
    await this.check_buffer();

    const rows =
      this.current_rows.map((r) => this.json_row_to_sql(r)).join(",\n") + ";\n";
    await this.write_to_stream(rows);
    await this.check_buffer();
    this.current_insert_count++;
    this.current_rows = [];
  }

  async new_stream() {
    // Creates a new filestream to write to. Also clears the file tracker
    if (this.ostream) {
      await new Promise((resolve, reject) => {
        this.ostream.end(() => resolve());
        this.ostream.on("error", reject);
      });
    }
    this.current_file_number++;
    const fname = path.join(
      this.dir,
      `${this.file_pre}${this.current_file_number}.sql`,
    );
    console.log(`Opening new write stream: ${fname}`);
    this.files.push(fname);
    this.ostream = fs.createWriteStream(fname);
    this.current_insert_count = 0;
  }

  json_row_to_sql(row) {
    // Function to process each JSON row and convert to SQL INSERT statement
    const formatValue = (value) => {
      if (value === null || value === "null") return "null";
      return `"${value.toString().replace(/"/g, '""')}"`;
    };

    let {
      hep_code = null,
      classification = null,
      assignment = null,
      doi = null,
      openalex_id = null,
      journal_era_id = null,
      journal_title = null,
      paper_title = null,
      authors = [],
      inst_authors = [],
      year = null,
      citations = null,
      apportionment = [],
    } = JSON.parse(JSON.stringify(row));
    // Process authors
    authors = authors.map((author) => {
      if (author.name) {
        author.name = author.name.replace(/'/g, "''");
      }
      const { raw_affiliation, ...authorWithoutRaw } = author;
      return authorWithoutRaw;
    });

    // Process institutional authors
    inst_authors = inst_authors.map((author) => {
      if (author.name) {
        author.name = author.name.replace(/'/g, "''");
      }
      const { raw_affiliation, ...authorWithoutRaw } = author;
      return authorWithoutRaw;
    });

    // Filter empty apportionment objects
    apportionment = apportionment.filter((app) => Object.keys(app).length > 0);

    // Format values for SQL
    const values = [
      formatValue(hep_code),
      formatValue(classification),
      formatValue(assignment),
      formatValue(doi),
      formatValue(openalex_id),
      formatValue(journal_era_id),
      formatValue(journal_title),
      formatValue(paper_title),
      `json('${JSON.stringify(inst_authors)}')`,
      `json('${JSON.stringify(authors)}')`,
      year === null ? "null" : year,
      citations === null ? "null" : citations,
      `json('${JSON.stringify(apportionment)}')`,
    ];

    return `(${values.join(", ")})`;
  }
}
module.exports = { SQLBatchWriter };
