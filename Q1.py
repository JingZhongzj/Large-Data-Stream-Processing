import os, sys
import re
from pyspark.sql import functions, SparkSession
from operator import add
from pyspark.sql.functions import *


log_file_path = '/Users/' + os.path.join('zhongjing', 'Downloads', 'epa-http.txt')
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
q1_df = log_df.select('host', 'bytes')

q1_rdd = q1_df.rdd
reduced_q1_rdd = q1_rdd.reduceByKey(add)

# Convert the rdd to DataFrame and show the answer.
reduced_q1_df = spark.createDataFrame(reduced_q1_rdd)
df_q1_ans = reduced_q1_df.select(col("_1").alias("host"), col("_2").alias("Total_number_of_bytes"))
df_q1_ans.show(n=20, truncate=False)
print 'This is the answer of question 1 \n\n\n'






