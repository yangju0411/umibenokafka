from kafka import KafkaProducer
from json import dumps
import time


class Producer:
    def __init__(self, broker, log_path):
        self.broker = broker

        self.file = open(log_path, "r")
        
        self.producer = KafkaProducer(acks = 0, compression_type = 'gzip',
                         bootstrap_servers = broker,
                         value_serializer = lambda x: dumps(x).encode('utf-8'))
    
    def deliver(self, last, topic):
        self.last = last
        self.file.seek(self.last)
        while True:
            where = self.file.tell()
            line = self.file.readline()
            if not line:
                time.sleep(1)
                self.file.seek(where)
            else:
                # print(line)
                self.producer.send(topic, value = line)
                self.producer.flush()
                self.last = self.file.tell()