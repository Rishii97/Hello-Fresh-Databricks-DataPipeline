# Databricks notebook source
import logging
from pyspark.sql import functions as F
from pyspark.sql import SparkSession

# Setting up logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger()

try:
    # Step 1: Setting up the Azure Blob Storage account key
    spark.conf.set("fs.azure.account.key.storageaccountda.dfs.core.windows.net", "Account Key")
    logger.info("Azure Blob Storage account key set.")

    # Step 2: Reading Parquet file
    data = spark.read.parquet("abfss://loggingprocessednew@storageaccountda.dfs.core.windows.net/finaldata.parquet")
    logger.info("Data read from Parquet file.")

    # Step 3: Filtering data for ingredients containing 'beef'
    data_filtered = data.filter(F.lower(data['ingredients']).contains("beef"))
    logger.info(f"Filtered data for ingredients containing 'beef'. Found {data_filtered.count()} rows.")

    # Step 4: Calculating total cooking time
    data_filtered = data_filtered.withColumn(
        'total_cook_time',
        F.col('cookTime_minutes') + F.col('prepTime_minutes')
    )
    logger.info("Calculated total cooking time.")

    # Step 5: Assigning difficulty level based on total cooking time
    data_filtered = data_filtered.withColumn(
        'difficulty',
        F.when(F.col('total_cook_time') < 30, 'easy')
        .when((F.col('total_cook_time') >= 30) & (F.col('total_cook_time') <= 60), 'medium')
        .otherwise('hard')
    )
    logger.info("Assigned difficulty level based on cooking time.")

    # Step 6: Grouping by difficulty and calculate average cooking time
    df_avg_cooking_time = data_filtered.groupBy('difficulty').agg(
        F.avg('total_cook_time').alias('avg_total_cooking_time')
    )
    logger.info("Calculated average cooking time for each difficulty level.")

    # Step 7: Writing results to CSV
    output_path = "abfss://loggingoutputnew@storageaccountda.dfs.core.windows.net/output.csv"
    df_avg_cooking_time.coalesce(1).write.mode("overwrite").option("header", "true").csv(output_path)
    logger.info(f"Data written to {output_path} in CSV format.")

except Exception as e:
    logger.error(f"Task 2 failed with error: {e}")
