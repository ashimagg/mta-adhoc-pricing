import datetime
import time
import threading
from properties import subwayLines, profileName
import csv 

import boto3
session = boto3.Session(profile_name=profileName)
kinesis = session.client('kinesis')



class KinesisProducer(threading.Thread):
	"""Producer class for AWS Kinesis streams

	This class will emit records with the IP addresses as partition key and
	the emission timestamps as data"""

	def __init__(self, stream_name, sleep_interval=None, partition_key='8.8.8.8'):
		self.stream_name = stream_name
		self.sleep_interval = sleep_interval
		self.partition_key = partition_key
		super().__init__()

	def put_record(self, data):
		part_key = self.partition_key
		kinesis.put_record(StreamName=self.stream_name,Data=data, PartitionKey = part_key)

	def read_file(self):
		"""put a record at regular intervals"""
		# while True:
		# 	self.put_record()
		# 	time.sleep(self.sleep_interval)		
		with open('../data/processed_data/'+self.partition_key+'.csv') as csvfile:
			readCSV = csv.reader(csvfile, delimiter='\n')
			for index,row in enumerate(readCSV):
				if index>0:
					print(row[0])
					# self.put_record(row[0])
					time.sleep(self.sleep_interval)

	def run(self):
		try:
			self.read_file()
		except Exception as e:
			print(e)
		
for line in subwayLines:
	KinesisProducer(line, sleep_interval=0, partition_key=line).start()



