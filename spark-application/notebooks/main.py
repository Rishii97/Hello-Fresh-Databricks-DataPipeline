# Databricks notebook source
import logging
from pyspark.sql import functions as F
from pyspark.sql.types import IntegerType
from fractions import Fraction
import re

# Step 1: Setting up logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger()

# Step 1: Setting up logging to console and file
log_file = "/Workspace/Projects/spark-application/logs/logfile.log"  # Specify the path where you want the log file

# Creating a logger
logger = logging.getLogger()
logger.setLevel(logging.INFO)

# Creating a console handler for output to console
console_handler = logging.StreamHandler()
console_handler.setLevel(logging.INFO)
console_formatter = logging.Formatter('%(asctime)s - %(levelname)s - %(message)s')
console_handler.setFormatter(console_formatter)

# Creating a file handler for logging to a file
file_handler = logging.FileHandler(log_file)
file_handler.setLevel(logging.INFO)
file_formatter = logging.Formatter('%(asctime)s - %(levelname)s - %(message)s')
file_handler.setFormatter(file_formatter)

# Adding both handlers to the logger
logger.addHandler(console_handler)
logger.addHandler(file_handler)

# Example logging message
logger.info("This is an info log message.")
logger.error("This is an error log message.")


# Step 2: Setting up the Azure Blob Storage account key
try:
    spark.conf.set("fs.azure.account.key.storageaccountda.dfs.core.windows.net", "lj4JmE78iNNv4OskFrXkiYX6st0lQESVmKdJslt4E/QpNlmekr/2Xoj/uE6u9S0vAW8LcHciAv7R+AStL6lvPQ==")
    logger.info("Azure Blob Storage account key set successfully.")
except Exception as e:
    logger.error(f"Error setting Azure Blob Storage account key: {e}")

# Step 3: Defining the path with wildcard to read all files in the folder
input_path = "abfss://input@storageaccountda.dfs.core.windows.net/*.json"
# Here input is the input folder containing all the json files

# Step 4: Reading all JSON files into a single DataFrame
try:
    data = spark.read.json(input_path)
    logger.info(f"Read data from input path: {input_path}")
except Exception as e:
    logger.error(f"Error reading data from input path {input_path}: {e}")

# Adding a column with the file names
data = data.withColumn("file_name", F.input_file_name())

# Step 5: Defining the function to convert ISO 8601 duration format to minutes
def iso_to_minutes(iso_str):
    try:
        if isinstance(iso_str, str):
            if 'M' in iso_str and 'H' in iso_str:
                hours = int(iso_str.split('H')[0].replace('PT', ''))
                minutes = int(iso_str.split('H')[1].replace('M', ''))
                return hours * 60 + minutes
            elif 'M' in iso_str:
                return int(iso_str.replace('PT', '').replace('M', ''))
            elif 'H' in iso_str:
                hours = int(iso_str.replace('PT', '').replace('H', ''))
                return hours * 60
        return 0
    except Exception as e:
        logger.error(f"Error processing ISO 8601 duration string: {iso_str}. Error: {e}")
        return 0

# Step 6: Registering the function as a UDF
iso_to_minutes_udf = F.udf(iso_to_minutes, IntegerType())

# Step 7: Applying the UDF to convert 'cookTime' and 'prepTime' columns
try:
    data = data.withColumn('cookTime_minutes', iso_to_minutes_udf(data['cookTime']))
    data = data.withColumn('prepTime_minutes', iso_to_minutes_udf(data['prepTime']))
    logger.info("Cook time and prep time columns converted to minutes.")
except Exception as e:
    logger.error(f"Error converting 'cookTime' and 'prepTime' columns to minutes: {e}")

# Step 8: Defining the recipeYield conversion function
def text_to_num(text):
    text_num_map = {
        "one": 1, "two": 2, "three": 3, "four": 4, "five": 5,
        "six": 6, "seven": 7, "eight": 8, "nine": 9, "ten": 10,
        "eleven": 11, "twelve": 12, "thirteen": 13, "fourteen": 14,
        "fifteen": 15, "sixteen": 16, "seventeen": 17, "eighteen": 18,
        "nineteen": 19, "twenty": 20
    }
    return text_num_map.get(text.lower(), None)

def parse_fraction(fraction_str):
    try:
        return float(Fraction(fraction_str))
    except ValueError:
        return None

def process_recipe_yield(yield_str):
    try:
        if isinstance(yield_str, str):
            yield_str = yield_str.lower().strip()
            dozen_count = 0
            if "dozen" in yield_str:
                dozen_match = re.search(r'(\d+\s\d+/\d+|\d+/\d+|\d+)(\sto\s\d+)?\sdozen', yield_str)
                if dozen_match:
                    dozen_part = dozen_match.group(1)
                    if " " in dozen_part:
                        whole, frac = dozen_part.split()
                        dozen_count = int(whole) + parse_fraction(frac)
                    elif "/" in dozen_part:
                        dozen_count = parse_fraction(dozen_part)
                    else:
                        dozen_count = int(dozen_part)
                    if dozen_match.group(2):
                        range_end = int(re.search(r'\d+', dozen_match.group(2)).group())
                        dozen_count = (dozen_count + range_end) / 2
                return int(dozen_count * 12)
            serves_match = re.search(r'(serves?|makes?about?)\s*(\d+\s*\d*/\d*|\d+|[a-z]+)\s*(to|or)?\s*(\d+\s*\d*/\d*|\d+|[a-z]+)?', yield_str)
            if serves_match:
                first_value = serves_match.group(2)
                if first_value.isdigit():
                    return int(first_value)
                else:
                    num = text_to_num(first_value)
                    if num is not None:
                        return num
                second_value = serves_match.group(4)
                if second_value:
                    if second_value.isdigit():
                        return (int(first_value) + int(second_value)) // 2
                    else:
                        num2 = text_to_num(second_value)
                        return (num + num2) // 2
            fraction_match = re.search(r'(\d+\s\d+/\d+|\d+/\d+|\d+)', yield_str)
            if fraction_match:
                fraction_part = fraction_match.group(1)
                if " " in fraction_part:
                    whole, frac = fraction_part.split()
                    return int(float(whole) + parse_fraction(frac))
                elif "/" in fraction_part:
                    return int(parse_fraction(fraction_part))
                else:
                    return int(fraction_part)
            range_match = re.search(r'(\d+)\sto\s(\d+)', yield_str)
            if range_match:
                start, end = map(int, range_match.groups())
                return (start + end) // 2
        return None
    except Exception as e:
        logger.error(f"Error processing yield value: {yield_str}. Error: {e}")
        return None

process_recipe_yield_udf = F.udf(process_recipe_yield, IntegerType())

# Step 9: Applying the yield conversion function
try:
    data = data.withColumn('recipeYield_numeric', process_recipe_yield_udf(data['recipeYield']))
    logger.info("Recipe yield converted to numeric values.")
except Exception as e:
    logger.error(f"Error converting 'recipeYield' column: {e}")

# Step 10: Computing the median per file for missing values
def compute_per_file_median(df, column_name):
    try:
        return df.groupBy("file_name").agg(F.expr(f'percentile_approx({column_name}, 0.5)').alias(f'{column_name}_median'))
    except Exception as e:
        logger.error(f"Error calculating median for {column_name}: {e}")
        return None

median_per_file_cook = compute_per_file_median(data, 'cookTime_minutes')
median_per_file_prep = compute_per_file_median(data, 'prepTime_minutes')
median_per_file_yield = compute_per_file_median(data, 'recipeYield_numeric')

# Joining medians back to DataFrame
try:
    data = data.join(median_per_file_cook, on='file_name', how='left') \
               .join(median_per_file_prep, on='file_name', how='left') \
               .join(median_per_file_yield, on='file_name', how='left')
    logger.info("Medians for missing values joined back to data.")
except Exception as e:
    logger.error(f"Error joining median values back to data: {e}")

# Step 11: Replacing missing or zero values with default or median values
try:
    data = data.withColumn(
        'cookTime_minutes',
        F.when((F.col('cookTime_minutes').isNull()) | (F.col('cookTime_minutes') == 0), 20).otherwise(F.col('cookTime_minutes'))
    ).withColumn(
        'prepTime_minutes',
        F.when((F.col('prepTime_minutes').isNull()) | (F.col('prepTime_minutes') == 0), 10).otherwise(F.col('prepTime_minutes'))
    ).withColumn(
        'recipeYield_numeric',
        F.when((F.col('recipeYield_numeric').isNull()) | (F.col('recipeYield_numeric') == 0), F.col('recipeYield_numeric_median')).otherwise(F.col('recipeYield_numeric'))
    )
    logger.info("Replaced missing or zero values with default or median values.")
except Exception as e:
    logger.error(f"Error replacing missing or zero values: {e}")

# Step 12: Selecting relevant columns and write to Parquet
try:
    data = data.filter(F.trim(F.col("name")) != "").select(
        "name", "description", "image", "ingredients", "recipeYield_numeric",
        "cookTime_minutes", "prepTime_minutes", "datePublished", "URL"
    )
    logger.info("Selected relevant columns.")
except Exception as e:
    logger.error(f"Error selecting relevant columns: {e}")

# Step 13: Defining the output path for the processed data
output_path = "abfss://loggingprocessednew2@storageaccountda.dfs.core.windows.net/finaldata.parquet"
# Here loggingprocessednew2 is the processed folder change the name with the suitable folder path

# Step 14: Writing the data to Parquet format
try:
    data.write.mode("overwrite").parquet(output_path)
    logger.info(f"Data written to Azure Blob Storage at {output_path} as Parquet.")
except Exception as e:
    logger.error(f"Error writing data to Parquet: {e}")

# Final logging message
logger.info("Data processing completed.")


import logging
from pyspark.sql import functions as F
from pyspark.sql import SparkSession

# Setting up logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger()

try:
    # Step 1: Setting up the Azure Blob Storage account key
    spark.conf.set("fs.azure.account.key.storageaccountda.dfs.core.windows.net", "lj4JmE78iNNv4OskFrXkiYX6st0lQESVmKdJslt4E/QpNlmekr/2Xoj/uE6u9S0vAW8LcHciAv7R+AStL6lvPQ==")
    logger.info("Azure Blob Storage account key set.")

    # Step 2: Reading Parquet file
    data = spark.read.parquet("abfss://loggingprocessednew2@storageaccountda.dfs.core.windows.net/finaldata.parquet")
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
    output_path = "abfss://loggingoutputnew2@storageaccountda.dfs.core.windows.net/output.csv"
    df_avg_cooking_time.coalesce(1).write.mode("overwrite").option("header", "true").csv(output_path)
    logger.info(f"Data written to {output_path} in CSV format.")
    # Loggingoutputnew2 is the final output folder, change the folder pathif required
except Exception as e:
    logger.error(f"Task 2 failed with error: {e}")



# COMMAND ----------

