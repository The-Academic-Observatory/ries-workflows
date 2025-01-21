# ./code/loaders

This directory contains ETL scripts (referred to as *telescopes* within COKI). Each script maps one external data source into a `meta.json` file and a `data.jsonl` file. The files are uploaded into a Google Cloud Storage bucket and are directly compatible with Google BigQuery (for future import).

Each script may be imported as a library, or run individually at the command line. For usage, run `node telescope_{name}.js --help`.

It's safe to (re)run a script at any time as existing assets will NOT be overwritten. To regenerate selected assets then delete, rename or move them prior to running.

| File | Description |
| - | - |
| telescope.js               | Generic ETL functionality, used by other scripts |
| telescope_coki.js          | Maps publication metadata from COKI's internal database (not public) |
| telescope_history.js   | Maps previous ERA ratings from <https://dataportal.arc.gov.au> |
| telescope_forcodes_2008.js | Maps ANZSRC 2008 FoR codes from <https://www.abs.gov.au> |
| telescope_forcodes_2020.js | Maps ANZSRC 2020 FoR codes from <http://aria.stats.govt.nz> |
| telescope_heps.js          | Maps a static set of higher-education providers sourced from the ARC |
| telescope_issns.js         | Maps an ISSN <-> ISSN-L mapping from <https://www.issn.org> |
| telescope_journals_2018.js | Maps the ERA 2018 Journal List from <https://web.archive.org.au> |
| telescope_journals_2023.js | Maps the ERA 2023 Journal List from <https://www.arc.gov.au> |
| telescope_rors.js          | Maps the Research Organisation Registry list from <https://zenodo.org> |
| telescope_une_generate.js          | Generates data for UNE-specific assignments |
| telescope_papers.js          | Extracts raw data from the COKI database|

Each ETL script has the following properties:

```js
{
  account : "",       // GCloud account with write permissions for Cloud Storage
  project : "ries",   // GCloud project name under which resources will be created
  bucket  : "",       // GCloud storage bucket name/path where files will be written
  workdir : ".cache", // Path to the local working directory
  name    : "",       // Unique name used as a table-name and for working directories
  schema  : {},       // BigQuery compatible data schema
}

// Example
{
  account : 'gcloud-user@your-domain.com',
  project : 'ries',
  bucket  : 'gs://my-bucket/ries_data',
  workdir : '/tmp/ries_cache',
  name    : 'forcodes',
  schema  : {  
    description : 'Official Field of Research (FoR) codes',
    fields : [
      { name:'code', type:'STRING' , description:'FoR 4-digit code' },
      { name:'name', type:'STRING' , description:'FoR title / name' }
    ]
  }
}

```

## Extraction Phase

Raw data files are downloaded from a third-party source into a local directory.

- a working directory will be created at `{workdir}/{name}`
- raw data files will be downloaded into `{workdir}/{name}/raw/*`
- existing files will NOT be replaced
- the directory may be deleted at any time (consider it as a temporary cache)

## Transformation Phase

Raw data files are mapped into BigQuery-compatible files.

- the data `{schema}` will be saved to `{workdir}/{name}/meta.json`
- transformed data will be saved to `{workdir}/{name}/data.jsonl`
- existing files will NOT be replaced

## Load Phase

Prepared files are uploaded into cloud storage, ready to be imported into BigQuery.

- this phase will be skipped if a valid `{gcloud.account}` is not provided
- `{gcloud.project}` will be created if required
- `{gcloud.bucket}` will be created if required
- the schema file will be copied to `{gcloud.bucket}/{name}/meta.json`
- the data file will be copied to `{gcloud.bucket}/{name}/data.jsonl`
- existing remote files will NOT be replaced

## Re-Running an ETL Script

Do you actually need to re-run an ETL script? Several remote datasets are static (do not change over time), so there is no need to regenerate files.

- by default, if you re-run a script then existing files will be detected and nothing will happen.
- if you delete any particular file(s), then the script will replace only the missing file(s).
- it's a good idea to backup previous files, just in case an ETL process fails.
- a good idea is to declare a versioned `{name}` for each ETL run. For example:

```bash
# append the date (YYYY_MM_DD) to version an ETL run
node telescope_rors.js \
  --account="your_user@your_domain.com" \
  --project="ries" \
  --bucket="gs://your_bucket/ries_data" \
  --workdir="/tmp/ries_data" \
  --name="rors_$(date "+%Y_%m_%d")" # rors_YYYY_MM_DD
