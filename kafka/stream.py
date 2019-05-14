# sudo /Users/aviator/spark-2.4.0-bin-hadoop2.7/bin/spark-submit --packages org.apache.spark:spark-streaming-kafka-0-8_2.11:2.4.2 stream.py
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

sc = SparkContext(appName="PythonSparkStreamingKafka_RM_01")
sc.setLogLevel("WARN")
sc.setLogLevel("ERROR")


sql_context = SQLContext(sc)

#will create batch for 30sec
ssc = StreamingContext(sc, 3)

kafkaStream = KafkaUtils.createStream(ssc, 'localhost:2181', 'subway-group', {'RLine':1})

parsed = kafkaStream.map(lambda v: v.split('\n'))
print(parsed)


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
    df = df.withColumn('price1', 10*df.entries*df.entries/F.sum("entries").over(window))
    df = df.withColumn('price2', (10*df.entries*df.entries/F.sum("entries").over(window) + 10*df.exits*df.exits/F.sum("exits").over(window)/2))

    return df 

def process(rdd):
    if not rdd.isEmpty():
        rows = toRow(rdd)
        #global exits
        
        #df = sql_context.createDataFrame(data=OrderedDict( { 'foo': pd.Series(foo), 'bar': pd.Series(bar) } ), schema=['id','stations','entries', 'exits'])
        cSchema = StructType([StructField("ID", IntegerType()),StructField("Station", StringType()),StructField("entries", DoubleType()),StructField("exits", DoubleType())])

        df = sql_context.createDataFrame(rows,schema=cSchema) 

        transformed_df = process_df(df)
        transformed_df.show()
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

'''
['func', 
'preservesPartitioning', 
'_prev_jrdd', '_prev_jrdd_deserializer', 'is_cached', 'is_checkpointed'
, 'ctx', 'prev', '_jrdd_val', '_id', '_jrdd_deserializer', '_bypass_serializer', 'partitioner', 'is_barrier', '__module__', '__doc__', '__init__',
 'getNumPartitions', '_jrdd', 'id', '_is_pipelinable', '_is_barrier', '_pickled', '__repr__', '__getnewargs__', 'context', 'cache', 'persist', 'unpersist', 'checkpoint', 'isCheckpointed', 'localCheckpoint', 
 'isLocallyCheckpointed', 'getCheckpointFile', 'map', 'flatMap', 'mapPartitions', 'mapPartitionsWithIndex', 'mapPartitionsWithSplit', 'filter', 'distinct', 'sample', 'randomSplit', 'takeSample', 
 '_computeFractionForSampleSize', 'union', 'intersection', '_reserialize', '__add__', 'repartitionAndSortWithinPartitions', 'sortByKey', 'sortBy', 'glom', 'cartesian', 'groupBy', 'pipe', 'foreach', 'foreachPartition', 'collect', 'reduce', 'treeReduce', 'fold', 'aggregate', 'treeAggregate', 'max', 'min', 'sum', 'count', 'stats', 'histogram', 'mean', 'variance', 'stdev', 'sampleStdev', 'sampleVariance', 'countByValue', 'top', 'takeOrdered', 'take', 'first', 'isEmpty', 'saveAsNewAPIHadoopDataset', 'saveAsNewAPIHadoopFile', 'saveAsHadoopDataset', 'saveAsHadoopFile', 'saveAsSequenceFile', 'saveAsPickleFile', 'saveAsTextFile', 'collectAsMap', 'keys', 'values', 'reduceByKey', 'reduceByKeyLocally', 'countByKey', 'join', 'leftOuterJoin', 'rightOuterJoin', 'fullOuterJoin', 'partitionBy', 'combineByKey', 'aggregateByKey', 'foldByKey', '_memory_limit', 'groupByKey', 'flatMapValues', 'mapValues', 'groupWith', 'cogroup', 'sampleByKey', 'subtractByKey', 'subtract', 'keyBy', 'repartition', 'coalesce', 'zip', 'zipWithIndex', 'zipWithUniqueId', 'name', 'setName', 'toDebugString', 'getStorageLevel', '_defaultReducePartitions', 'lookup', '_to_java_object_rdd', 'countApprox', 'sumApprox', 'meanApprox', 'countApproxDistinct', 'toLocalIterator', 'barrier', '__dict__', '__weakref__', 'toDF', '__hash__', '__str__', '__getattribute__', '__setattr__', '__delattr__', '__lt__', '__le__', '__eq__', '__ne__', '__gt__', '__ge__', '__new__', '__reduce_ex__', '__reduce__', '__subclasshook__', '__init_subclass__', '__format__', '__sizeof__', '__dir__', '__class__']
 '''
