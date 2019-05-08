from kafka import KafkaConsumer

from properties import subwayLines
import threading

class Kafka(threading.Thread):
	def __init__(self, stream_name, sleep_interval=None):
		self.stream_name = stream_name
		self.sleep_interval = sleep_interval
		
		self.buffer = {}
		self.buffer[stream_name] = []
		
		super().__init__()

	def read_record(self):
		consumer = KafkaConsumer(self.stream_name, bootstrap_servers='localhost:9092')
		for msg in consumer:
			self.buffer[self.stream_name].append(msg.value.decode().split(","))
			print(self.stream_name, msg.value.decode())

		print("#########################")	
	
	def run(self):
		try:
			self.read_record()
			self.process_buffer()
		except Exception as e:
			print(e)

	def process_buffer(self):
		print(self.buffer[self.stream_name])

# for line in subwayLines:
# 	Kafka(line, sleep_interval=0).start()
Kafka("RLine", sleep_interval=0).start()