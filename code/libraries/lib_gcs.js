/*
## Description
Simple wrappers around Google Cloud Storage
TODO: link gcloud settings to app settings, eg, default credentials file and project id

## Contacts
julian.tonti-filippini@curtin.edu.au

## License
Apache 2.0
*/
const sh = require("shelljs");
sh.config.silent = true;
const { resolve } = require("path");

// copy a file (use gs:// to indicate cloud location origin)
function copy(src, dest, project) {
  sh.exec(`gcloud storage cp --project='${project}' ${src} ${dest}`);
}

function push(local, remote) {
  console.log(`gcloud storage cp ${resolve(local)} gs://${remote}`);
  return sh.exec(`gcloud storage cp ${resolve(local)} gs://${remote}`);
}

// pull a file from the cloud
function pull(remote, local) {
  sh.exec(`gcloud storage cp gs://${remote} ${resolve(local)}`);
}

// list available objects in the cloud
function ls(remote = "", project = "", recursive = false) {
  return sh
    .exec(
      `gcloud alpha storage ls --project='${project}' ${recursive ? "--recursive" : ""} 'gs://${remote}'`,
    )
    .trim()
    .split("\n");
}
module.exports = { copy, push, pull, ls };
