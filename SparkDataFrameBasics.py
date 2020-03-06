#!/usr/bin/env python
from pyspark.sql import SparkSession
from pyspark.sql.functions import format_number
from pyspark.sql.functions import max, min
from pyspark.sql.functions import mean
from pyspark.sql.functions import corr
from pyspark.sql.functions import year
from pyspark.sql.functions import month

print("Start of exercise")

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

"""
Bonus Question!
There are too many decimal places for mean and stddev in the describe() dataframe. 
Format the numbers to just show up to two decimal places. Pay careful attention to the datatypes 
that .describe() returns, we didn't cover how to do this exact formatting, but we covered 
something very similar. Check this link for a hint¶
"""
print("\n","Format the numbers to just show up to two decimal places")
result = df.describe()
print(result.select(result['summary'],
              format_number(result['Open'].cast('float'),2).alias('Open'),
              format_number(result['High'].cast('float'), 2).alias('High'),
              format_number(result['Low'].cast('float'), 2).alias('Low'),
              format_number(result['Close'].cast('float'), 2).alias('Close'),
              result['Volume'].cast('int').alias('Volume')
              ).show())

"""
Create a new dataframe with a column called HV Ratio that is the ratio of the High Price versus volume of stock traded for a day.¶
"""
print("\nCreate a new dataframe with a column called HV Ratio that is the ratio of the High Price versus volume of stock traded for a day.")
df2 = df.withColumn("HV Ratio",df['High']/df['Volume'])
print(df2.select('HV Ratio').show())

"""
What day had the Peak High in Price?
"""
print("\nWhat day had the Peak High in Price?")
print(df.orderBy(df['High'].desc()).head(1)[0][0])

"""
What is the mean of the Close column?
"""
print("\nWhat is the mean of the Close column?")
print(df.select(mean('Close')).show())

"""
What is the max and min of the Volume column?
"""
print("\nWhat is the max and min of the Volume column?")
print(df.select(max('Volume'),min('Volume')).show())

"""How many days was the Close lower than 60 dollars?"""
print("\nHow many days was the Close lower than 60 dollars?")
print(df.filter(df['Close'] < 60).count())

"""What percentage of the time was the High greater than 80 dollars ?"""
print("\nWhat percentage of the time was the High greater than 80 dollars?")
print((df.filter(df['High']>80).count()/df.count()) * 100)

"""What is the Pearson correlation between High and Volume?"""
print("\nWhat is the Pearson correlation between High and Volume?")
print(df.select(corr('High','Volume')).show())

"""What is the max High per year?"""
print("\nWhat is the max High per year?")
yeardf = df.withColumn("Year",year(df['Date']))
max_df = yeardf.groupBy('Year').max()
print(max_df.select('Year','max(High)').show())

"""
What is the average Close for each Calendar Month?
In other words, across all the years, what is the average Close price for Jan,Feb, Mar, etc... Your result will have a value for each of these months.¶
"""
print("\nWhat is the average Close for each Calendar Month?")
print("In other words, across all the years, what is the average Close price for Jan,Feb, Mar, etc... Your result will have a value for each of these months.")
monthdf = df.withColumn('Month',month('Date'))
monthavgs = monthdf.select(['Month','Close']).groupBy('Month').mean()
print(monthavgs.select('Month','avg(Close)').orderBy('Month').show())

print("End of exercise")