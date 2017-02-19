import os, sys
import re
from pyspark.sql import functions
from operator import add
from pyspark.sql.functions import *


log_file_path = '/Users/' + os.path.join('your path')
print "The path of the log file is: " + log_file_path

#Read the log file.
df = sqlContext.read.text(log_file_path)

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
q2_df = log_df.select('host', 'bytes')

q2_rdd = q2_df.rdd
reduced_q2_rdd = q2_rdd.reduceByKey(add)
reduced_q2_df = sqlContext.createDataFrame(reduced_q2_rdd)\
                .select(col("_1").alias("host"), col("_2").alias("Total_number_of_bytes"))

reduced_q2_df_sorted = reduced_q2_df.orderBy(reduced_q2_df.Total_number_of_bytes.desc())


# Homework Q2: Return the top-K IPs that were served the most number of bytes
# Sort the content_size.

reduced_q2_df_sorted.show(n=int(raw_input("Please input k: ")), truncate=False)
