#!/usr/bin/env bash

# Function to display usage
usage() {
    echo "Creates service account permissions for running the RIES Workflow"
    echo "Usage: $0 PROJECT COKI_PROJECT SERVICE_ACCOUNT"
    exit 1
}

# Check if the number of arguments is exactly 3
if [ "$#" -ne 3 ]; then
    usage
fi

# Assign arguments to variables
project=$1
coki_project=$2
service_account=$3
echo "Working Google Cloud Project: $project"
echo "COKI Google Cloud Project: $coki_project"
echo "Service Account Principal: $service_account"

# Create the RIES Workflow Role. A custom role is created so that we don't provide more permissions than necessary.
permissions="bigquery.datasets.create,bigquery.jobs.create,bigquery.tables.create,bigquery.tables.createSnapshot,bigquery.tables.delete,bigquery.tables.deleteSnapshot,bigquery.tables.get,bigquery.tables.getData,bigquery.tables.list,bigquery.tables.update,bigquery.tables.updateData,storage.objects.list"
if [[ $(gcloud iam roles list --project=${project} --filter="name:RIESWorkflowRole") == "Listed 0 items." ]]; then
    echo "RIESWorkflowRole doesn't exist for ${project}. Creating role"
    gcloud iam roles create RIESWorkflowRole --project=$project \
        --title="RIES Workflow Role" \
        --description="Gives necessary permissions to run the RIES workflow" \
        --permissions=$permissions
else
    echo "RIESWorkflowRole for ${project} already exists. running update"
    gcloud iam roles update RIESWorkflowRole --project=$project \
        --title="RIES Workflow Role" \
        --description="Gives necessary permissions to run the RIES workflow" \
        --permissions=$permissions
fi

# Add roles for SA to query the coki project
gcloud projects add-iam-policy-binding $coki_project \
    --member=serviceAccount:$service_account \
    --role="roles/bigquery.dataViewer" \

    gcloud projects add-iam-policy-binding $coki_project \
    --member=serviceAccount:$service_account \
    --role="roles/bigquery.jobUser"\

    # Add Role to our service account
gcloud projects add-iam-policy-binding $project \
    --member=serviceAccount:$service_account \
    --role=projects/$project/roles/RIESWorkflowRole \

    # Create a bucket
bucket="${project}-ries"
gsutil ls gs://$bucket &>/dev/null
if [ $? -ne 0 ]; then
    echo "Creating bucket: ${project}-ries"
    gcloud storage buckets create gs://${project}-ries --location=US
else
    echo "Bucket ${bucket} already exists"
fi

# Give Service account access to bucket (project-ries)
gsutil iam ch \
    serviceAccount:$service_account:roles/storage.legacyBucketReader \
    serviceAccount:$service_account:roles/storage.objectCreator \
    serviceAccount:$service_account:roles/storage.objectViewer \
    gs://${project}-ries

echo "All done!"
