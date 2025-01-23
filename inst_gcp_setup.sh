#!/usr/bin/env bash

# Function to display usage
usage() {
    echo "Gives a service account the necessary permissions to create RIES tables in an institutional project"
    echo "Usage: $0 PROJECT SERVICE_ACCOUNT"
    exit 1
}

# Check if the number of arguments is exactly 2
if [ "$#" -ne 2 ]; then
    usage
fi

# Assign arguments to variables
project=$1
service_account=$2
echo "Institution RIES project: $project"
echo "Service Account Principal: $service_account"

# Create the RIES Workflow Role. A custom role is created so that we don't provide more permissions than necessary.
permissions="bigquery.datasets.create,bigquery.jobs.create,bigquery.tables.create,bigquery.tables.createSnapshot,bigquery.tables.delete,bigquery.tables.deleteSnapshot,bigquery.tables.get,bigquery.tables.getData,bigquery.tables.list,bigquery.tables.update,bigquery.tables.updateData,storage.objects.list"
output=$(gcloud iam roles list --project=${project} --filter="name:RIESWorkflowRole" 2>&1)
if [[ $output == *"Listed 0 items"* ]]; then
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

    # Add Role to our service account
gcloud projects add-iam-policy-binding $project \
    --member=serviceAccount:$service_account \
    --role=projects/$project/roles/RIESWorkflowRole \

