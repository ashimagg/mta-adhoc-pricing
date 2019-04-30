
import boto3
session = boto3.Session(profile_name='ashim')
kinesis = session.client('kinesis')
import time
from properties import subwayLines

for line in subwayLines:
    response = kinesis.create_stream(StreamName=line, ShardCount=1)
    print(response)
    time.sleep(5)