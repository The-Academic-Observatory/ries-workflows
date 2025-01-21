/*
## Description
Wraps gcloud CLI

## Contacts
julian.tonti-filippini@curtin.edu.au

## License
Apache 2.0
*/
const exec = require("./lib_exec");

/*
Global flags that are always useful
--account   : choose an account
--project   : choose a project
--billing-project
--flatten   : flattens arrays
--format    : json(fields), values
--quiet     : disables interactive prompts, reverting to defaults
--verbosity : error
*/

function GCloud() {
  function gc(command) {
    const account = api.account ? `--account="${api.account}"` : "";
    const project = api.project ? `--project="${api.project}"` : "";
    const gcloud = `gcloud ${account} ${project} ${command}`;
    let [err, out] = exec(gcloud);
    err = err ? err.trim().split("\n").join(" ") : null;
    out = err ? null : JSON.parse(out);
    let result = [];
    if (err) {
      if (err.startsWith("ERROR")) result = [err, null];
      else if (err.startsWith("WARNING")) result = [err, null];
      else result = [null, err];
    } else {
      result = [null, out];
    }
    if (api.verbose) {
      console.log("REQ:", gcloud);
      if (result[0]) console.error("ERR:", result[0]);
      if (result[1]) console.log("RES:", result[1]);
    }
    return result;
  }
  const api = {
    account: "your_account",
    project: "your_project",
    verbose: false,
    config: {
      list: () => gc(`config list --format='json'`)[1],
      get: (k) => gc(`config get  --format='json' '${k}'`)[1],
      set: (k, v) => gc(`config set  --format='json' '${k}' '${v}'`)[1],
    },
    auth: {
      list: () => gc(`auth list --format='json'`)[1],
      active: () => gc(`auth list --format='json' --filter='status:active'`)[1],
      login: (s) => gc(`auth login "${s}"`)[1],
    },
    service_account: {
      list: () => gc(`iam service-accounts list     --format='json'`)[1],
      exists: (s) =>
        gc(`iam service-accounts describe --format='json' '${s}'`)[0] === null,
      describe: (s) =>
        gc(`iam service-accounts describe --format='json' '${s}'`)[1],
      create: (s) => gc(`iam service-accounts create   --format='json' '${s}'`),
      delete: (s) => gc(`iam service-accounts delete   --format='json' '${s}'`),
      activate: (s) => gc(`auth activate-service-account "${s}"`)[1],
    },
    projects: {
      list: () => gc(`projects list     --format='json'`)[1],
      exists: (s) => gc(`projects describe --format='json' "${s}"`)[0] === null,
      describe: (s) => gc(`projects describe --format='json' "${s}"`)[1],
      create: (s) => gc(`projects create   --format='json' "${s}"`)[1],
      delete: (s) => gc(`projects delete   --format='json' --quiet "${s}"`),
    },
    dataset: {
      list: () => gc(`alpha bq datasets list     --format='json'`)[1],
      exists: (s) =>
        gc(`alpha bq datasets describe --format='json' "${s}"`)[0] === null,
      describe: (s) =>
        gc(`alpha bq datasets describe --format='json' "${s}"`)[1],
      create: (s) => gc(`alpha bq datasets create   --format='json' "${s}"`)[1],
      delete: (s) =>
        gc(`alpha bq datasets delete   --format='json' --quiet "${s}"`),
    },
    // table : {
    //   list     : (d)   => gc(`alpha bq tables list     --format='json' --dataset='${d}'`)[1],
    //   exists   : (d,t) => gc(`alpha bq tables describe --format='json' --dataset='${d}' '${t}'`)[0] === null,
    //   describe : (d,t) => bq(`show --dataset='${d}' '${t}'`)[1],
    //   delete   : (t)   => bq(`rm --table "${t}"`),
    //   query    : (q)   => bq(`query "${s}"`),
    //   create   : (q)   => bq(`query --destination_schema="${schema_file} --destination_table="${table} --description="${description} '${query}'`),
    //   load     : ()    => bq(`load --replace --source_format='NEWLINE_DELIMITED_JSON' "${table}" "${bucket} "${schema_file}" `),
    // },
    buckets: {
      list: () => gc(`storage buckets list     --format='json'`)[1],
      exists: (s) =>
        gc(`storage buckets describe --format='json' "${s}"`)[0] === null,
      describe: (s) => gc(`storage buckets describe --format='json' "${s}"`)[1],
      create: (s) => gc(`storage buckets create   --format='json' "${s}"`)[1],
      delete: (s) => gc(`storage buckets delete   --format='json' "${s}"`),
    },
    file: {
      list: (s) => gc(`storage ls "${s}"`)[1],
      exists: (s) =>
        gc(`storage objects describe --format="json" "${s}"`)[0] === null,
      describe: (s) => gc(`storage objects describe --format="json" "${s}"`)[1],
      //create   : (s)   => sh('echo "" | gc(`storage cp - "${s}"`)[1]),
      delete: (s) => gc(`storage rm "${s}"`)[1],
      cat: (s) => gc(`storage cat "${s}"`)[1],
      copy: (s, d) => gc(`storage cp "${s}" "${d}"`)[1],
      move: (s, d) => gc(`storage mv "${s}" "${d}"`)[1],
      hash: (s) => gc(`storage hash --format="json" "${s}"`)[1],
    },
    roles: {
      // roles must be fully specified, ie: "roles/biquery.admin", not "bigquery.admin"
      all: () =>
        gc(`iam roles list --format="json(name,title,description)"`)[1],
      get: () =>
        gc(
          `projects get-iam-policy $(gcloud config get project) --flatten="bindings[].members" --format='json(bindings.role)' --filter="bindings.members:$(gcloud config get account)"`,
        )[1],
      has: (r) =>
        gc(
          `projects get-iam-policy $(gcloud config get project) --flatten="bindings[].members" --format='json(bindings.role)' --filter="bindings.members:$(gcloud config get account) AND bindings.role:${r}"`,
        )[1],
      add: (s) =>
        gc(
          `projects add-iam-policy-binding $(gcloud config get project) --member="$(gcloud config get account) --role="${s}"`,
        ),
    },
  };
  return api;
}
module.exports = GCloud;
