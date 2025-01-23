# Installation

The application requires [NodeJS] with various command line tools. If you wish to run queries and build an online database then you will also require access to a [Google BigQuery][BigQuery] instance (details below).

### Install Script
Among other things, RIES requires the `gcloud` and command line utility. For information on installing and setting up `gcloud`, refer to the official Google resources: [GCloud][gcloud]

```bash
# run the install script
bash install.sh 
```

This will check that the relevant command line packages are installed, then install the project dependencies with `npm` and `pnpm`.

### Keyfile
```bash
# Create the keyfile symlink. The keyfile is your service account access to Google Cloud Platform services
ln -s /path/to/your/keyfile.json .keyfile.json
```
Creates a symbolic link to a keyfile. This is your access to Google Cloud Platform services via a service account. For more information on GCP authentication and service accounts, see [Service Accounts][service accounts], [Service Account Keys][service account keys] and [IAM].

Example of what a service accouint key looks like:
```json
{
  "type": "YOUR GCLOUD ACCOUNT TYPE",
  "project_id": "YOUR PROJECT ID",
  "private_key_id": "YOUR KEY ID",
  "private_key": "-----BEGIN PRIVATE KEY----- YOUR KEY TEXT -----END PRIVATE KEY-----\n",
  "client_email": "YOUR GCLOUD EMAIL ADDRESS",
  "client_id": "YOUR GCLOUD USER ID",
  "auth_uri": "https://accounts.google.com/o/oauth2/auth",
  "token_uri": "https://oauth2.googleapis.com/token",
  "auth_provider_x509_cert_url": "https://www.googleapis.com/oauth2/v1/certs",
  "client_x509_cert_url": "https://www.googleapis.com/robot/v1/metadata/x509/YOUR CERTIFICATE ADDRESS"
}
```

### GCP permissions
```bash
# Set up various GCP resources and permissions
bash gcp_setup.sh my_project coki_project service_account_principal

# Set up any institutional projects
bash inst_gcp_setup.sh inst_project service_account_principal
```

In order for your service account to interact with your project's various resources, it requires specific permissions on GCP. The RIES workflow also makes use of COKI resources, which are a part of the COKI GCP project. The final argument is the service account principal. This is the email address identifier that your service account uses. This can be found on GCP or in your linked keyfile under *client_email*.

In a production setting, the workflow will also attempt to write institutional datasets to their respective Bigquery projects. For this reason, we need to also provide permissions for access to these projects with the `inst_gcp_setup.sh` script.

At this point, you should have a generic config file at .config.json. A properly conctructed config file is essential to running the RIES workflow. For further information on the file's contents, see [configuration]. Make sure your config file is set up before proceeding.

### Telescopes
```bash
# If this is the first run for this project:
node telescope/index.js
```
The RIES workflow depends on some specific data to be available. This data needs to be pulled from its source only once and is not updated depending on the config parameters. As such, the scripts that harvest the data are part of their own directory and are separate to the main workflow. They need to be run at least once for the project before the main workflow is run.

### Run the Workflow
```bash
node ries.js
```
If all has gone to plan, the main workflow can now be run!

<!-- links -->
[NodeJS]: <https://nodejs.org/en/download/>
[BigQuery]: <https://cloud.google.com/bigquery/>
[IAM]: <https://cloud.google.com/iam>
[keyfile]: <https://cloud.google.com/bigquery/docs/authentication/service-account-file>
[JSON]: <https://www.json.org/json-en.html>
[configuration]: <configuration.md>
[service accounts]: <https://cloud.google.com/iam/docs/service-account-overview>
[service account keys]: <https://cloud.google.com/iam/docs/service-account-creds#key-types>
[gcloud]: <https://cloud.google.com/sdk/docs/install>
