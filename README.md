# HelloFresh Recipe Data Processing Pipeline



This project processes recipe data, including cook time, preparation time, and yield conversion, using Apache Spark and Azure Blob Storage. The pipeline reads JSON files from Azure Blob Storage, processes the data, and writes the results to a Parquet file. The pipeline includes logging, error handling, and specific data transformations, such as converting ISO 8601 durations and adjusting recipe yields. After writing the Parquet file and reading it back, the data is filtered to include only recipes that contain "beef" in the ingredients column.

The script also attempts to calculate the `total cooking time` by summing the `cookTime_minutes` and `prepTime_minutes` columns. Finally, the data is aggregated based on the `difficulty level`, and the `average cooking time` for each difficulty level is calculated and saved in CSV format in the final output folder.
## Prerequisites

- Apache Spark (PySpark)
- Azure Blob Storage account
- Databricks workspace (optional for Databricks users)
- Python 3.x
- `pyspark`, `fractions`, `re`, `logging` libraries
- Azure Storage account keys

## Requirements to run locally:
1 . **Install Apache Spark:**

-  Download and install Apache Spark on your local machine. Ensure the version matches the one used in Databricks.
-  Set the SPARK_HOME environment variable to point to the Spark installation directory.
2 .  **Install Java:**

-  Apache Spark requires Java. Install a compatible version (e.g., Java 8 or Java 11).
- Set the JAVA_HOME environment variable to point to the Java installation directory.
3 . **Python Environment:**

- Install Python (version 3.8 or above is recommended).
- Create a virtual environment to manage dependencies.
4 . **Azure Dependencies:**

- Include libraries for connecting to Azure Blob Storage, such as azure-storage-blob.
5 . **Databricks Connect (Optional):**

- If you want to interact with Databricks clusters from your local machine, install databricks-connect and configure it with your Databricks workspace.
6 . **Data File Path:**

- Replace Databricks-specific paths (e.g., /dbfs) with local or Azure Blob Storage paths in your code.

## Setup
1 . **Create a virtual  environment:**
```bash
python -m venv venv
source venv/bin/activate  # On Windows, use venv\Scripts\activate
```

1 . **Install Dependencies:**

   To run this project locally, you need to have Apache Spark and the necessary libraries installed.
   ```bash
   pip install -r requirements.txt
   ```
   You can install Apache Spark and PySpark using pip:

   ```bash
   pyspark==3.3.2  # Match this to the version used on Databricks
   pandas>=1.3.0
   numpy>=1.20.0
   azure-storage-blob>=12.9.0
   azure-identity>=1.5.0
   pyyaml>=6.0  # For reading configuration files
   databricks-connect==13.1.0  # Optional, only if connecting to   Databricks from local
   ```
2 .**Running the project locally: Run the Spark job locally by executing:**

```bash
python main.py
```
2 . **Azure Blob Storage:**
NOTE: 
You will need an Azure Blob Storage account for reading and writing data.
- Create an Azure Storage account if you don’t already have one.
- Get the account key from your Azure portal.

3 . **Set up your Azure Storage Configuration:**
In the `spark.conf.set` method in the code, replace the Azure Blob Storage account key and storage path with your own:

   ```bash 
   spark.conf.set("fs.azure.account.key.<your_storage_account_name>.dfs.core.windows.net", "<your_account_key>")
   ```
4 . **Review the Logs:**

The logs are written both to the console and to a log file located at:
```bash
/Workspace/Projects/spark-application/logs/logfile.log
```
You can customize the log file location based on your preference.
### Databricks Setup:
1 . **Set up Databricks Workspace:**

- Create a Databricks workspace if you don’t already have one.
-  Upload the script and data files to Databricks or use Azure Blob Storage as the data source.
2 . **Set up Azure Blob Storage in Databricks:** 

In Databricks, you can set up your Azure Blob Storage account using the dbutils to mount storage.

Example code to mount the Azure Blob Storage container to Databricks:

```bash
spark.conf.set("fs.azure.account.key.<storageaccountname>.dfs.core.windows.net", "<storageaccountkey>")
```
Make sure you replace `<your_container>`, `<your_storage_account>`, and `<scope>/<key>` with your actual storage details.

3 . **Run the Code in Databricks:**

- Open your Databricks workspace and create a new notebook.
- Paste the code into the notebook.
- Adjust the file paths and storage credentials accordingly.
- Run the notebook to process the data.

4  . **Input Location:** 
```bash
input_path = "abfss://input@<storageaccountname>.dfs.core.windows.net/*.json"
```

4 . **Output Proceesed Location:**

The task 1 output will be stored in Azure Blob Storage in Parquet format. The output path is specified in the script:
```bash
output_path = "abfss://loggingprocessednew2@storageaccountda.dfs.core.windows.net/<file_name>.parquet"
```
For the task 2, the code reads the Parquet file that was written as the output in Task 1. This file is stored in Azure Data Lake Storage (using the ABFS protocol) under the path:
```bash
input_path = "abfss://<processeddatapath>@storageaccountda.dfs.core.windows.net/<file_name>.parquet"
```
The final output of the data pipeline will be saved in the final output folder
```bash
output_path =abfss://<finaloutputpath>@storageaccountda.dfs.core.windows.net/output.csv 
```
Make sure to set up access permissions and replace the storage path with your own.

### Features
1 . **Logging:** The script uses logging to track the progress and errors during execution. Logs are stored both in the console and in a log file.

2 . **Data Processing:**
- Reads JSON data from Azure Blob Storage.
- Converts ISO 8601 durations for cookTime and prepTime into minutes.
- Converts recipe yield (e.g., "makes 4 servings") into numeric values.
- Handles missing or zero values by replacing them with defaults or computed medians.
3 . **Output:** The processed data is written to a Parquet file in Azure Blob Storage.

### Data Processing Steps: 
1 . **Reading Data:** Data is read from the JSON files in Azure Blob Storage using a wildcard path.

2 . **Processing Fields:** `cookTime` and `prepTime` are converted from ISO 8601 duration format into numeric values representing minutes. If the fields are empty, a default value or computed median is assigned.

- Example:
  - Input: `PT30M` (ISO 8601 format for 30 minutes)
  - Output: `30` (numeric minutes)

3 . **Recipe Yield Conversion:**
   - Converts textual representations of recipe yields (e.g., "makes 4 servings") into numeric values.
   - Handles fractional values (e.g., "1/2 cups") and converts them to their decimal equivalents.
   - If yield data is missing, default or median values are used.

4 . **Handling Missing or Invalid Data:**
   - Replaces missing `cookTime` or `prepTime` with default values or the dataset median.
   - Assigns default numeric values for recipe yields when missing.

5 . **Data Writing:**
   - The processed data is saved to Azure Blob Storage in Parquet format.
   - The final data is saved in CSV format as a task  2 result.
   - Parquet format for the processed data is chosen for its efficient storage and query performance for large datasets.

### Customization
1 . **Changing Default Values:** Update the default or median values for cookTime, prepTime, or recipeYield in the code.

2 . **Logging Configuration:** Modify the logging level or log file path in the script.

3 . **Output Path:** Change the output_path variable to write the processed data to a different location.

### Troubleshooting
1 . **Error:** Azure Storage Key Not Found Ensure you have set the correct Azure Storage key in the spark.conf.set method or Databricks secrets.

2 . **Error:** Cannot Mount Storage in Databricks Verify that the dbutils.fs.mount command has the correct source path and access credentials.

3 . **Error:** Invalid JSON Format Validate the input JSON files for proper structure and required fields

## Schedulling the Data Pipeline in Azure Databricks:
The data pipeline can be scheduled using the `Azure Databricks` or by `Apache Airflow`. 

Steps to schedule the pipeline in Databricks is as follow:
- Create a Notebook/Job: Use your data pipeline logic Databricks notebook.

- Create a Job: Go to Jobs in Databricks.
- Click Create Job and select Notebook or another task type.
- Choose your notebook or script.
- Select an existing cluster or create a new one to run the job.
- Set Schedule: Under Schedule, define when the job should run (e.g., daily, weekly, or with a cron expression).
-  Notifications: Set up email notifications for success/failure.
- Save and Run:
- Click Create to save and schedule the job.

Monitor: Track job status and logs in the Jobs tab.
This will automate your data pipeline execution on the defined schedule.

## Implementing CI/CD Pipeline using Azure Devops:
1 . Setting up the Version Control with Git: In Databricks, by going to the User Settings and configuring the Git integration by connecting to GitHub.

2 .Build CI Pipeline:
   - Choose a CI/CD tool: Using the Azure Devops for creating the CI/CD pipeline.
   - Set up pipeline:
      - On Azure DevOps, creating a pipeline that listens for changes (pushes/commits) to the repository.
      - By using a YAML file or the DevOps UI to define the pipeline.
- The CI pipeline should:
   - Install necessary dependencies.
   - Run unit tests on notebooks or scripts to ensure the code is working correctly.
   - Use a tool like pytest for testing Python-based notebooks/scripts.
**Example of a basic YAML configuration for Azure DevOps:** 
```bash
trigger:
  branches:
    include:
      - main

pool:
  vmImage: 'ubuntu-latest'

steps:
- task: Checkout@1
- task: UsePythonVersion@0
  inputs:
    versionSpec: '3.x'
- script: |
    pip install -r requirements.txt
    pytest tests/
  displayName: 'Install dependencies and run tests'
```
3 .Build CD Pipeline:

- The CD pipeline automates the deployment process:
   - Deploy to Databricks: After the CI pipeline completes successfully, the CD pipeline deploys the updated notebooks or scripts to Databricks.
   - We can deploy via Databricks REST API or Databricks CLI. Set up tasks in Azure DevOps or GitHub Actions to:
      - Upload notebooks/scripts to a Databricks workspace.
      -  Trigger the Databricks job for execution.
**Example of deploying with Databricks CLI:**

```bash
databricks workspace import_dir ./notebooks /Workspace/my_folder --overwrite
```

