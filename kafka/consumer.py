from kafka import KafkaConsumer

from properties import subwayLines
import threading

class Kafka(threading.Thread):
	def __init__(self, stream_name, sleep_interval=None):
		self.stream_name = stream_name
		self.sleep_interval = sleep_interval
		super().__init__()

	def read_record(self):
		consumer = KafkaConsumer(self.stream_name, bootstrap_servers='localhost:9092')
		for msg in consumer:
			print(self.stream_name, msg.value.decode())
			print("\n\n\n\n\n\n")

	def run(self):
		try:
			self.read_record()
		except Exception as e:
			print(e)

# for line in subwayLines:
# 	Kafka(line, sleep_interval=0).start()
Kafka("RLine", sleep_interval=0).start()