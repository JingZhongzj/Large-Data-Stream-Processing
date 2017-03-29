import os, sys
import re
from pyspark.sql import functions, SparkSession
from operator import add
from pyspark.sql.functions import *
import argparse
from pyspark.sql.window import Window


def main(k):
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

    # Set a prefix
    prefix = u'141.243.1.'

    log_rdd = log_df.rdd

    # Select the hosts that has length larger than the prefix
    log_rdd_new = log_rdd.map(lambda x: (x, len(x[0])))\
                  .filter(lambda t: t[1]>=10)\
                  .map(lambda s: s[0])

    # Select the hosts that has the correct prefix
    q4_rdd_match = log_rdd_new.map(lambda x: (x, x[0][:10]))\
                    .filter(lambda t: t[1]==prefix)\
                    .map(lambda s: s[0])

    # Cache the rdd
    q4_rdd_match.cache()

    # Convert the rdd to the DataFrame
    q4_df_match = spark.createDataFrame(q4_rdd_match)

    # Select the relevant columns
    q4_1_df = q4_df_match.select('host', 'bytes')
    q4_1_rdd = q4_1_df.rdd

    # (1). Calculate the total number of bytes served to that IP address.
    reduced_q4_rdd = q4_1_rdd.reduceByKey(add)
    reduced_q4_df = spark.createDataFrame(reduced_q4_rdd)\
                   .select(col("_1").alias("host"), col("_2").alias("Total_number_of_bytes"))
    reduced_q4_df.show(n=reduced_q4_df.count(), truncate=False)
    print 'This is the answer of question 1 for the subnet with prefix 141.243.1.*\n\n\n'


    # (2). Return the top-K IPs that were served the most number of bytes.
    # Sort the content_size.
    reduced_q4_df_sorted = reduced_q4_df.orderBy(reduced_q4_df.Total_number_of_bytes.desc())
    reduced_q4_df_sorted.show(n=k, truncate=False)
    print 'This is the answer of question 2 for the subnet with prefix 141.243.1.*\n\n\n'


    # (3).
    q4_3_df = q4_df_match.select('host', 'bytes', 'time')
    q4_3_df = q4_3_df.withColumn('day:hour', q4_3_df.time[0:5]).select('*')
    q4_new_df = q4_3_df.withColumn('day:hour', q4_3_df.time[0:5]).select('*')

    windowSpec = Window\
            .partitionBy('host')\
            .orderBy('day:hour')

    q4_new_df.withColumn('total_bytes',\
                         sum('bytes')\
                         .over(windowSpec))\
                         .select('host', 'day:hour', 'total_bytes')\
                         .distinct().show(n=50, truncate=False)
    print 'This is the answer of question 3 for the subnet with prefix 141.243.1.*\n\n\n'





if __name__ == '__main__':
    parser = argparse.ArgumentParser()
    parser.add_argument("--ngrams", help="Input value k.")
    args = parser.parse_args()
    if args.ngrams:
        ngrams = args.ngrams
    main(int(ngrams))






