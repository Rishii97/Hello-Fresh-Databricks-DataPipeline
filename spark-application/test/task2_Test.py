# Databricks notebook source
import unittest
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, when, lower

class TestTask2(unittest.TestCase):

    @classmethod
    def setUpClass(cls):
        cls.spark = SparkSession.builder.master("local[1]").appName("UnitTests").getOrCreate()

    @classmethod
    def tearDownClass(cls):
        cls.spark.stop()  # Properly stop the SparkSession

    def test_filter_beef_ingredient(self):
        data = [("Beef stew", "beef, potatoes, carrots", 20, 15),
                ("Chicken curry", "chicken, potatoes, spices", 10, 20)]
        columns = ["name", "ingredients", "cookTime_minutes", "prepTime_minutes"]

        df = self.spark.createDataFrame(data, columns)
        df_filtered = df.filter(lower(df['ingredients']).contains("beef"))

        result = df_filtered.collect()

        self.assertEqual(len(result), 1)  # Only one recipe contains "beef"
        self.assertEqual(result[0]["name"], "Beef stew")

    def test_calculate_total_cooking_time(self):
        data = [("Beef stew", "beef, potatoes", 20, 15),
                ("Chicken curry", "chicken, potatoes", 10, 20)]
        columns = ["name", "ingredients", "cookTime_minutes", "prepTime_minutes"]

        df = self.spark.createDataFrame(data, columns)
        df_with_total_time = df.withColumn(
            'total_cook_time',
            col('cookTime_minutes') + col('prepTime_minutes')
        )

        result = df_with_total_time.collect()

        self.assertEqual(result[0]["total_cook_time"], 35)  # 20 + 15
        self.assertEqual(result[1]["total_cook_time"], 30)  # 10 + 20

    def test_assign_difficulty(self):
        data = [("Beef stew", "beef, potatoes", 20, 15),
                ("Chicken curry", "chicken, potatoes", 10, 20)]
        columns = ["name", "ingredients", "cookTime_minutes", "prepTime_minutes"]

        df = self.spark.createDataFrame(data, columns)
        df_with_difficulty = df.withColumn(
            'difficulty',
            when(col('total_cook_time') < 30, 'easy')
            .when((col('total_cook_time') >= 30) & (col('total_cook_time') <= 60), 'medium')
            .otherwise('hard')
        )

        result = df_with_difficulty.collect()

        self.assertEqual(result[0]["difficulty"], "easy")  # 35 minutes -> easy
        self.assertEqual(result[1]["difficulty"], "medium")  # 30 minutes -> medium

if __name__ == "__main__":
    # Manually running the tests
    suite = unittest.TestLoader().loadTestsFromTestCase(TestTask2)
    unittest.TextTestRunner().run(suite)
