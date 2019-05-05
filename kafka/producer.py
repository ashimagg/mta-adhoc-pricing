from kafka import KafkaProducer
import os
from time import sleep
import threading
from properties import subwayLines
import csv 
import time

producer = KafkaProducer(bootstrap_servers='localhost:9092')

class KafkaProducer(threading.Thread):
	def __init__(self, stream_name, sleep_interval=None):
		self.stream_name = stream_name
		self.sleep_interval = sleep_interval
		super().__init__()

	def put_record(self, data):
		dataItems = ""
		for item in data:
			dataItems+=item[0]+"\n"

		producer.send(self.stream_name, dataItems.encode())
		
	def read_file(self):
		with open('../data/processed_data/'+self.stream_name+'.csv') as csvfile:
			readCSV = csv.reader(csvfile, delimiter='\n')
			prevTime = 0
			timeSlot = []

			for index,row in enumerate(readCSV):
				if index>0:
					currenTime=row[0].split(',')[3]
					if(prevTime != currenTime):
						if(timeSlot):
							self.put_record(timeSlot)
						time.sleep(self.sleep_interval)
						timeSlot = []
						prevTime = currenTime
						timeSlot.append(row)
					else:
						timeSlot.append(row)
					


	def run(self):
		try:
			self.read_file()
		except Exception as e:
			print(e)


for line in subwayLines:
	KafkaProducer(line, sleep_interval=0.5).start()

# KafkaProducer("RLine", sleep_interval=0.5).start()