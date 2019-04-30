from kafka import KafkaProducer
import os
from time import sleep
import threading
from properties import subwayLines
import csv 
import time

producer = KafkaProducer(bootstrap_servers='localhost:9092')

class KafkaProducer(threading.Thread):
	"""Producer class for AWS Kinesis streams

	This class will emit records with the IP addresses as partition key and
	the emission timestamps as data"""

	def __init__(self, stream_name, sleep_interval=None):
		self.stream_name = stream_name
		self.sleep_interval = sleep_interval
		super().__init__()

	def put_record(self, data):
		producer.send(self.stream_name, data.encode())
		
	def read_file(self):
		"""put a record at regular intervals"""
		# while True:
		# 	self.put_record()
		# 	time.sleep(self.sleep_interval)		
		with open('../data/processed_data/'+self.stream_name+'.csv') as csvfile:
			readCSV = csv.reader(csvfile, delimiter='\n')
			for index,row in enumerate(readCSV):
				if index>0:
					self.put_record(row[0])
					# print(row[0])
					time.sleep(self.sleep_interval)

	def run(self):
		try:
			self.read_file()
		except Exception as e:
			print(e)



for line in subwayLines:
	KafkaProducer(line, sleep_interval=1).start()