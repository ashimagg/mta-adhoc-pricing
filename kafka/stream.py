# sudo /Users/aviator/spark-2.4.0-bin-hadoop2.7/bin/spark-submit --packages org.apache.spark:spark-streaming-kafka-0-8_2.11:2.4.2,org.mongodb.spark:mongo-spark-connector_2.11:2.4.0 stream.py
# Mac sux
import os
os.environ['PYSPARK_SUBMIT_ARGS'] = '--packages org.apache.spark:spark-streaming-kafka-0-8_2.11:2.4.2 pyspark-shell'
from pyspark.sql import SQLContext, Row, SparkSession
from pyspark import SparkContext, SparkConf
from pyspark.streaming import StreamingContext
from pyspark.streaming.kafka import KafkaUtils
from pyspark.sql import SQLContext
from pyspark.sql.types import *
from pyspark.sql.functions import *
from collections import OrderedDict



from pyspark import SparkContext

from pyspark.streaming import StreamingContext
from pyspark.streaming.kafka import KafkaUtils

import json

conf = SparkConf().setAppName("mtaStationAnalysis")
spark = SparkSession \
        .builder\
        .appName("streamingPubg") \
        .config("spark.mongodb.input.uri", "mongodb://127.0.0.1:27017/test") \
        .config("spark.mongodb.output.uri", "mongodb://127.0.0.1:27017/test") \
        .getOrCreate()
sc = spark.sparkContext

sql_context = SQLContext(sc)
sc.setLogLevel("ERROR")

'''
import json

sc = SparkContext(appName="PythonSparkStreamingKafka_RM_01")
sc.setLogLevel("WARN")
sc.setLogLevel("ERROR")


sql_context = SQLContext(sc)
'''
#will create batch for 30sec
ssc = StreamingContext(sc, 20)

kafkaStream = KafkaUtils.createStream(ssc, 'localhost:2181', 'subway-group', {'RLine':1})

parsed = kafkaStream.map(lambda v: v.split('\n'))
print(parsed)

def write_to_mongo(df):
    #df.show()
    #df.write.format("com.mongodb.spark.sql.DefaultSource").mode("append").option("database","mta").option("collection", "line").save()
    # df01 = spark.read\
    # .format("com.mongodb.spark.sql.DefaultSource")\
    # .option("database","test")\
    # .option("collection", "stations")\
    # .load()
    # df01.show()
    
    df.write.format("com.mongodb.spark.sql.DefaultSource")\
        .mode("overwrite")\
        .option("database","test")\
        .option("collection", "stations")\
        .save()


def toRow(records):
    #global exits

    rows = []
    rdd_str = records.take(records.count())
    record_list = rdd_str[0].split('\n')
    #print(record_list)
    id_ = 0
    for record in record_list:
        record_set = record.split(",")
        id_ += 1
        if len(record_set)>1:    
            rows.append([id_,record_set[0],float(record_set[1]),float(record_set[2])])
    return rows

def process_df(df):
    df = df.withColumn("entries", df["entries"].cast(DoubleType()))
    df = df.withColumn("exits", df["exits"].cast(DoubleType()))
    
    from pyspark.sql.window import Window
    window = Window.orderBy("id").rangeBetween(-2, 2)
    from pyspark.sql import functions as F
    df = df.withColumn('price1', round(df.entries*df.entries/F.sum("entries").over(window),2))
    df = df.withColumn('price2', round((df.entries*df.entries/F.sum("entries").over(window) + df.exits*df.exits/F.sum("exits").over(window)/2),2))

    return df 

def process(rdd):
    if not rdd.isEmpty():
        rows = toRow(rdd)
        #global exits
        
        #df = sql_context.createDataFrame(data=OrderedDict( { 'foo': pd.Series(foo), 'bar': pd.Series(bar) } ), schema=['id','stations','entries', 'exits'])
        cSchema = StructType([StructField("ID", IntegerType()),StructField("Station_name", StringType()),StructField("entries", DoubleType()),StructField("exits", DoubleType())])

        df = sql_context.createDataFrame(rows,schema=cSchema) 

        transformed_df = process_df(df)
        transformed_df.show()
        write_to_mongo(transformed_df)
        #df = rdd.toDF()
        #print(rdd.__dir__())
    else:
        print("waiting for producer")

lines = kafkaStream.map(lambda x: x[1]) 
lines.foreachRDD(process) 

#lines.pprint()


ssc.start()
#will terminate after 3 min
ssc.awaitTermination(timeout=180)
