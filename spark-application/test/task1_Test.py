# Databricks notebook source
import unittest
from pyspark.sql import SparkSession
from pyspark.sql.functions import udf
from pyspark.sql.types import IntegerType

class TestTask1(unittest.TestCase):

    @classmethod
    def setUpClass(cls):
        cls.spark = SparkSession.builder.master("local[1]").appName("UnitTests").getOrCreate()

    def test_iso_to_minutes(self):
        # Testing iso_to_minutes function
        def iso_to_minutes(iso_str):
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
        
        iso_to_minutes_udf = udf(iso_to_minutes, IntegerType())

        # Sample data to test
        data = [("PT1H30M",), ("PT20M",), ("PT2H",), ("PT0M",)]
        columns = ["cookTime"]

        df = self.spark.createDataFrame(data, columns)
        df_with_minutes = df.withColumn("cookTime_minutes", iso_to_minutes_udf(df["cookTime"]))
        
        result = df_with_minutes.collect()

        self.assertEqual(result[0]["cookTime_minutes"], 90)  # PT1H30M -> 90 minutes
        self.assertEqual(result[1]["cookTime_minutes"], 20)  # PT20M -> 20 minutes
        self.assertEqual(result[2]["cookTime_minutes"], 120)  # PT2H -> 120 minutes
        self.assertEqual(result[3]["cookTime_minutes"], 0)  # PT0M -> 0 minutes

    def test_recipe_yield_conversion(self):
        def text_to_num(text):
            text_num_map = {
                "one": 1, "two": 2, "three": 3, "four": 4, "five": 5,
                "six": 6, "seven": 7, "eight": 8, "nine": 9, "ten": 10,
                "eleven": 11, "twelve": 12, "thirteen": 13, "fourteen": 14,
                "fifteen": 15, "sixteen": 16, "seventeen": 17, "eighteen": 18,
                "nineteen": 19, "twenty": 20
            }
            return text_num_map.get(text.lower(), None)
        
        def process_recipe_yield(yield_str):
            try:
                if isinstance(yield_str, str):
                    yield_str = yield_str.lower().strip()

                    # Handling common phrases like "Makes 1 dozen" or "Serves 2 to 4"
                    if "dozen" in yield_str:
                        return 12  # Return 12 for "Makes 1 dozen"
                    elif "serves" in yield_str:
                        # Example: "Serves 2 to 4", extract the range
                        parts = yield_str.split("to")
                        if len(parts) == 2:
                            return (int(parts[0].split()[-1]) + int(parts[1].strip())) // 2  # Return the average
                    elif "makes" in yield_str:
                        # Look for numbers in the text and convert
                        words = yield_str.split()
                        for word in words:
                            if word.isdigit():
                                return int(word)
                return None  # Return None if no valid conversion
            except Exception as e:
                print(f"Error processing yield value: {yield_str}. Error: {e}")
                return None

        # Apply UDFs and transformations
        process_recipe_yield_udf = udf(process_recipe_yield, IntegerType())

        data = [("Serves 2 to 4",), ("Makes 1 dozen",)]
        columns = ["recipeYield"]

        df = self.spark.createDataFrame(data, columns)
        df_with_yield = df.withColumn("recipeYield_numeric", process_recipe_yield_udf(df["recipeYield"]))
        
        result = df_with_yield.collect()

        self.assertEqual(result[0]["recipeYield_numeric"], 3)  # Example expected value for "Serves 2 to 4"
        self.assertEqual(result[1]["recipeYield_numeric"], 12)  # Expected value for "Makes 1 dozen"

    def test_missing_value_handling(self):
        from pyspark.sql.functions import col, when

        data = [("1", 30, 15), (None, None, None)]
        columns = ["name", "cookTime_minutes", "prepTime_minutes"]

        df = self.spark.createDataFrame(data, columns)
        df_filled = df.withColumn(
            "cookTime_minutes", 
            when(col('cookTime_minutes').isNull(), 20).otherwise(col('cookTime_minutes'))
        ).withColumn(
            "prepTime_minutes", 
            when(col('prepTime_minutes').isNull(), 10).otherwise(col('prepTime_minutes'))
        )

        result = df_filled.collect()

        self.assertEqual(result[0]["cookTime_minutes"], 30)  # Value should remain the same for non-null
        self.assertEqual(result[1]["cookTime_minutes"], 20)  # Default value should be used for null
        self.assertEqual(result[1]["prepTime_minutes"], 10)  # Default value should be used for null

if __name__ == "__main__":
    # Manually running the tests in Databricks environment
    suite = unittest.TestLoader().loadTestsFromTestCase(TestTask1)
    runner = unittest.TextTestRunner()
    runner.run(suite)
