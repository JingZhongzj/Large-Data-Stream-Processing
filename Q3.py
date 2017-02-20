import os, sys
import re
from pyspark.sql import functions, SparkSession
from operator import add
from pyspark.sql.functions import *
from pyspark.sql.window import Window


log_file_path = '/Users/' + os.path.join('your path')
print "The path of the log file is: " + log_file_path

#Read the log file.
spark = SparkSession.builder.master('local').appName('Q1').config('spark.some.config.option', 'some_value').getOrCreate()
df = spark.read.text(log_file_path)

#Parse the dataframe.
split_df = df.select(functions.regexp_extract('value', r'^([^\s]+\s)', 1).alias('host'), \
                     functions.regexp_extract('value', r'^.*\[(\d{2}:\d{2}:\d{2}:\d{2})]', 1).alias('time'), \
                     functions.regexp_extract('value', r'^.*"(\w+\s+[^\s]+\s+HTTP.*)"', 1).alias('request'), \
                     functions.regexp_extract('value', r'^.*"\s+([^\s]+)', 1).cast('integer').alias('HTTP_reply_code'), \
                     functions.regexp_extract('value', r'^.*\s+(\d+)$', 1).cast('integer').alias('bytes'))

bad_host = split_df.filter(split_df.host.isNull())
bad_time = split_df.filter(split_df.time.isNull())
bad_request = split_df.filter(split_df.request.isNull())
bad_HTTP_reply_code = split_df.filter(split_df.HTTP_reply_code.isNull())
bad_bytes = split_df.filter(split_df.bytes.isNull())

bad_col = [bad_host, bad_time, bad_request, bad_HTTP_reply_code, bad_bytes]
col_name = ['host', 'time', 'request', 'HTTP_reply_code', 'bytes']

for i in range(len(bad_col)):
    if (bad_col[i].count() != 0):
        print "Column \"" + col_name[i] + "\" contains null value"

# Replace null content_size values with 0.
log_df = split_df.fillna({'bytes': 0})
q3_df = log_df.select('host', 'bytes', 'time')

# Change the time format
q3_df = q3_df.withColumn('day:hour_time', q3_df.time[0:5]).select('*')
q3_new_df = q3_df.withColumn('day:hour_time', q3_df.time[0:5]).select('*')

# Define the window
windowSpec = Window\
            .partitionBy('host')\
            .orderBy('day:hour_time')

# Calculate the total bytes in the window
q3_new_df.withColumn('total_bytes',\
                     sum('bytes').over(windowSpec)).select('host', 'day:hour_time', 'total_bytes').distinct().show(n=10000, truncate=False)









