import datetime
import time
import threading

import boto3
session = boto3.Session(profile_name='ashim')
kinesis = session.client('kinesis')




# from kinesis.exceptions import ResourceNotFoundException

class KinesisProducer(threading.Thread):
	"""Producer class for AWS Kinesis streams

	This class will emit records with the IP addresses as partition key and
	the emission timestamps as data"""

	def __init__(self, stream_name, sleep_interval=None, ip_addr='8.8.8.8'):
		self.stream_name = stream_name
		self.sleep_interval = sleep_interval
		self.ip_addr = ip_addr
		super().__init__()

	def put_record(self):
		"""put a single record to the stream"""
		timestamp = datetime.datetime.utcnow()
		part_key = self.ip_addr
		data = str(part_key)+" : "+str(timestamp.isoformat()) 
		kinesis.put_record(StreamName=self.stream_name,Data=data, PartitionKey = part_key)

	def run_continously(self):
		"""put a record at regular intervals"""
		while True:
			self.put_record()
			time.sleep(self.sleep_interval)

	def run(self):
		"""run the producer"""
		try:
			if self.sleep_interval:
				self.run_continously()
			else:
				self.put_record()
		except Exception as e:
			print(e)
			# print('stream {} not found. Exiting'.format(self.stream_name))
		# except ResourceNotFoundException:
		#     print('stream {} not found. Exiting'.format(self.stream_name))

producer1 = KinesisProducer("mta-data", sleep_interval=2, ip_addr='8.8.8.8')
producer2 = KinesisProducer("mta-data", sleep_interval=2, ip_addr='0.0.0.0')
# producer2 = KinesisProducer("mta-data", sleep_interval=5, ip_addr='8.8.8.9')
producer1.start()
producer2.start()
