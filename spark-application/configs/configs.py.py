# Databricks notebook source
# configs.


# Azure Blob Storage configurations
STORAGE_ACCOUNT_KEY = "Account Key "
STORAGE_ACCOUNT_NAME = "Account Name"
CONTAINER_NAME_INPUT = "Input Container Name"
CONTAINER_NAME_OUTPUT = "Output Container Name"
CONTAINER_NAME_LOGGING = "Container Logging Name"

# Input and Output paths
INPUT_PATH = f"abfss://{CONTAINER_NAME_INPUT}@{STORAGE_ACCOUNT_NAME}.dfs.core.windows.net/*.json"
OUTPUT_PATH_PARQUET = f"abfss://{CONTAINER_NAME_OUTPUT}@{STORAGE_ACCOUNT_NAME}.dfs.core.windows.net/finaldata.parquet"
OUTPUT_PATH_CSV = f"abfss://{CONTAINER_NAME_LOGGING}@{STORAGE_ACCOUNT_NAME}.dfs.core.windows.net/output.csv"
