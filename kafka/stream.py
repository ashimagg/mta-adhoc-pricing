from pyspark import SparkContext
from pyspark.streaming import StreamingContext
from pyspark.streaming.kafka import KafkaUtils
import json
sc = SparkContext(appName="PythonSparkStreamingKafka_RM_01")
ssc = StreamingContext(sc, 60)
brokers = ["localhost:9092"]
topic = "RLine"
directKafkaStream = KafkaUtils.createDirectStream(ssc, [topic], {"metadata.broker.list": brokers})
print(directKafkaStream)