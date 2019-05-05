# sudo /Users/aviator/spark-2.4.0-bin-hadoop2.7/bin/spark-submit --packages org.apache.spark:spark-streaming-kafka-0-8_2.11:2.4.2 stream.py

import os
os.environ['PYSPARK_SUBMIT_ARGS'] = '--packages org.apache.spark:spark-streaming-kafka-0-8_2.11:2.4.2 pyspark-shell'

from pyspark import SparkContext
from pyspark.streaming import StreamingContext
from pyspark.streaming.kafka import KafkaUtils

import json

sc = SparkContext(appName="PythonSparkStreamingKafka_RM_01")
# sc.setLogLevel("WARN")
sc.setLogLevel("ERROR")

#will create batch for 30sec
ssc = StreamingContext(sc, 30)

kafkaStream = KafkaUtils.createStream(ssc, 'localhost:2181', 'subway-group', {'RLine':1})

kafkaStream.pprint()

ssc.start()
#will terminate after 3 min
ssc.awaitTermination(timeout=180)


