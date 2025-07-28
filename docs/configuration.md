# Configuration

The RIES workflow relies on a config file to source its run parameters. This is not included in the repository, so it must be created by the user. Below is a guide for the available settings and their function.

## Available Settings

These are the available settings (key-pairs). Additional settings may be added to the settings object and they will be passed through as-is. All settings may be overridden at the command line by prefixing with a double dash, for example `--start 2010` or `--start=2010`

| name                    | type     | description                                                                                     |
| ----------------------- | -------- | ----------------------------------------------------------------------------------------------- |
| project                 | text     | The name of your BigQuery project. Will shard all tables with this date.                        |
| version                 | date     | The Version of this RIES run. If Left blank, will use the doi_table_version.                    |
| replace                 | boolean  | Overwrite existing tables when compiling. Can be left blank for no overrides.                   |
| institutional_hep_codes | array    | A list of HEP codes with Institutional data.                                                    |
| output_heps_filter      | array    | A list of HEP codes to filter for when constructing the hep ouptuts. If empty, will not filter. |
| start                   | integer  | Start analysis at this year (inclusive).                                                        |
| finish                  | integer  | Finish analysis at this year (inclusive).                                                       |
| inst_project_override   | text     | A project to override the institutional outputs with.                                           |
| coki_project            | text     | The name of the COKI project to pull data from.                                                 |
| coki_dataset            | text     | The name of the COKI dataset to pull data from.                                                 |
| doi_table_version       | date     | The sharded date of the doi table.                                                              |
| keyfile                 | filepath | Path to your BigQuery keyfile.                                                                  |
| compile_ries_queries    | boolean  | Run parameter - whether to compile the core RIES queries.                                       |
| run_ries_queries        | boolean  | Run parameter - whether to run the compiled core RIES queries.                                  |
| export_ries_tables      | boolean  | Run parameter - whether to export the core RIES tables.                                         |
| run_output_queries      | boolean  | Run parameter - whether to run the RIES output queries.                                         |
| run_output_queries      | boolean  | Run parameter - whether to export the RIES output data.                                         |

## Config File

The config file (located at `.config.json`) should contain the above settings. See [[config_example.json]] for a more detailed example of what the config file should look like.
