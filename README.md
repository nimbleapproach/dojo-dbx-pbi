
# Databricks Deployment and Azure Pipeline Configuration Guide

## Introduction

This repository provides a set of templates for deploying resources to Databricks and configuring Azure pipelines. The main configuration files include `databricks.yml`, `databricks.resources.jobs.yml`, and `azure_pipeline.yaml`.

## Adjusting Configuration for Databricks

### `databricks.yml`

This file provides an overview of the Databricks environment, workspace settings, and associated resources. Here's how to adjust it:

1. **Bundle Name**: Update the `name` field under `bundle` to provide a unique identifier for your set of Databricks resources.
2. **Variables**: Adjust the `variables` section to set default values for various parameters used in your Databricks jobs or notebooks.
3. **Workspace & Environments**: The `workspace` and `targets` sections configure details for different Databricks environments (e.g., development, staging, production) and should not be changed.

### `databricks.resources.jobs.yml`

This file outlines the configuration and settings for Databricks jobs. Here's how to customize it:

1. **Job Name**: Under the `jobs` list, update the `name` field for each job to provide a unique identifier.
2. **Cluster Configuration**: Adjust the `new_cluster` section to define the Spark version, node type, and number of workers for the cluster associated with the job.
3. **Libraries**: If your job requires specific libraries, configure them under the `libraries` section in first task. For instance, Python packages from PyPI can be added under the `- pypi` list.
4. **Job Scheduling**: The `schedule` section allows you to set up a cron schedule for the job. Adjust the `quartz_cron_expression` and `timezone_id` fields as needed.

## Configuring Azure Pipeline

For setting up the Azure pipeline, use the provided `azure_pipeline.yaml` file. Use that file from git repo in the Azure DevOps UI.

**Note**: Ensure that you have the necessary Azure DevOps permissions to create and manage pipelines, and that your Azure DevOps environment has access to the necessary resources.

---


## Setting Up Azure Pipeline using Azure DevOps UI

### 1. Navigate to Azure DevOps

- Open your browser and navigate to [Azure DevOps](https://dev.azure.com/).
- Sign in using your Microsoft account.

### 2. Select or Create a Project

- Once you're in Azure DevOps, select the desired organization from the drop-down list.
- Choose an existing project or create a new one by clicking on the `+ New Project` button.

### 3. Access Pipelines

- In the left-hand sidebar, click on `Pipelines`.

### 4. Create a New Pipeline

- Click on the `+ New` button and then select `New Pipeline`.

### 5. Configure Your Repository

- Azure DevOps will ask where your code is located. Choose GitHub.
- Select the repository that contains your code and the `azure_pipeline.yaml` file.

### 6. Configure the Pipeline

- Azure DevOps will auto-detect the `azure_pipeline.yaml` file in your repository.
- Review the pipeline YAML that Azure DevOps presents. It should match the content of your `azure_pipeline.yaml` file.

### 7. Run and Save

- Click on the `Run` button to execute a test run of your pipeline.
- If everything looks good, click on `Save`.

### 8. Monitor Pipeline Runs

- Once saved, you'll be taken to the pipeline overview page. Here, you can monitor the status of your pipeline runs, view logs, and troubleshoot if necessary.
After first run pipeline will ask for granting necessary permission.

### 9. Additional Configurations (Optional)

- If you need to add variables, triggers, or any other configurations:
  - Click on the `Edit` button on the pipeline overview page.
  - Use the UI options to configure as needed and save your changes.
    

**Note**: For this pipeline to work correctly, it's essential to follow a specific branch strategy. Understanding and implementing a proper branching strategy ensures smooth and efficient CI/CD processes. Please follow branch startegy from [external resource](https://asdauk.atlassian.net/wiki/spaces/AP/pages/298059208/branch+protection+strategy).
