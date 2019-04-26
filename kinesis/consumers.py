from boto.kinesis.exceptions import ProvisionedThroughputExceededException
import datetime

class KinesisConsumer:
    """Generic Consumer for Amazon Kinesis Streams"""
    def __init__(self, stream_name, shard_id, iterator_type,
                 worker_time=30, sleep_interval=0.5):

        self.stream_name = stream_name
        self.shard_id = str(shard_id)
        self.iterator_type = iterator_type
        self.worker_time = worker_time
        self.sleep_interval = sleep_interval

    def process_records(self, records):
        """the main logic of the Consumer that needs to be implemented"""
        raise NotImplementedError

    @staticmethod
    def iter_records(records):
        for record in records:
            part_key = record['PartitionKey']
            data = record['Data']
            yield part_key, data

    def run(self):
        """poll stream for new records and pass them to process_records method"""
        response = kinesis.get_shard_iterator(self.stream_name,
            self.shard_id, self.iterator_type)

        next_iterator = response['ShardIterator']

        start = datetime.datetime.now()
        finish = start + datetime.timedelta(seconds=self.worker_time)

        while finish > datetime.datetime.now():
            try:
                response = kinesis.get_records(next_iterator, limit=25)

                records = response['Records']

                if records:
                    self.process_records(records)

                next_iterator = response['NextShardIterator']
                time.sleep(self.sleep_interval)
            except ProvisionedThroughputExceededException as ptee:
                time.sleep(1)