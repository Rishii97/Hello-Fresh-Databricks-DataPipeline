# Databricks notebook source
#Importing all the required libraries
import logging
from pyspark.sql import functions as F
from pyspark.sql.types import IntegerType
from fractions import Fraction
import re

# Step 1: Setting up logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger()

# Step 2: Setting up the Azure Blob Storage account key
try: #using the azure access key to accces the containers from databricks
    spark.conf.set("fs.azure.account.key.storageaccountda.dfs.core.windows.net", "Account Key")
    logger.info("Azure Blob Storage account key set successfully.")
except Exception as e:
    logger.error(f"Error setting Azure Blob Storage account key: {e}")

# Step 3: Defining the path with wildcard to read all files in the folder
input_path = "abfss://input@storageaccountda.dfs.core.windows.net/*.json" #input folder in the container with all the json files

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

# Step 8: Defining the recipeYield conversion function (Data Cleaning for the RecipeYield Column)
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
#Defining the function extracting the numbers present in the string in RecipeYield column
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
output_path = "abfss://loggingprocessednew@storageaccountda.dfs.core.windows.net/finaldata.parquet"

# Step 14: Writing the data to Parquet format
try:
    data.write.mode("overwrite").parquet(output_path)
    logger.info(f"Data written to Azure Blob Storage at {output_path} as Parquet.")
except Exception as e:
    logger.error(f"Error writing data to Parquet: {e}")

# Final logging message
logger.info("Data processing completed.")
