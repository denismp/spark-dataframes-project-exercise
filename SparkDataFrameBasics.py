#!/usr/bin/env python
from pyspark.sql import SparkSession

"""
Use the walmart_stock.csv file to Answer and complete the tasks below!
Start a simple Spark Session¶
"""
spark_session = SparkSession.builder.appName('Basics').getOrCreate()

"""
Load the Walmart Stock CSV File, have Spark infer the data types.
"""
df = spark_session.read.csv("walmart_stock.csv",header=True,inferSchema=True)

"""
What are the column names?
"""
print("What are the column names?")
columns_list = df.columns
print(columns_list)

"""
What does the Schema look like?"
"""
print("What does the Schema look like?")
df.printSchema()

"""
Print out the first 5 columns.
"""
print("Print out the first 5 columns")
print(df.head(5))
print("\nEach row individually:")
for row in df.head(5):
    print(row)

"""
Use describe() to learn about the DataFrame.¶
"""
print("\nUse describe() to learn about the DataFrame.\n",type(df.describe()))
print(df.describe().show())
print("Hello world")