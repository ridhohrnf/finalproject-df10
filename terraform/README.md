# Step-by-step Guide

## Part 1: Cloud Resource Provisioning with Terraform

1. Install `gcloud` SDK, `terraform` CLI, and create a GCP project. Then, create a service account with **Storage Admin**, **Storage Pbject Admin**, and **BigQuery Admin** role. Download the JSON credential and store it on `.google/credentials/google_credential.json`. Open `01_terraform/main.tf` in a text editor, and change `dataanalyticscourse-327913` with your GCP's project id.

2. Enable IAM API and IAM Credential API in GCP.

3. Change directory to `01_terraform` by executing
```
cd 01_terraform
```

4. Initialize Terraform (set up environment and install Google provider)
```
terraform init
```
5. Plan Terraform infrastructure creation
```
terraform plan
```
6. Create new infrastructure by applying Terraform plan
```
terraform apply
```
7. Check GCP console to see newly-created resources.
