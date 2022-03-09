from kafka import KafkaProducer 
from json import dumps

import argparse
import time
import atexit

parser = argparse.ArgumentParser()

parser.add_argument("--name", "-n", required = True, help = "name of job", nargs = 1)
parser.add_argument("--path", "-p", required = True, help = "path of log file", nargs = 1)
parser.add_argument("--topic", "-t", required = True, help = "kafka Topic", nargs = 1)
parser.add_argument("--broker", "-b", required = True, help = "kafka Brokers", nargs = "+")

args = parser.parse_args()
log_path = args.path[0]
last_path = args.name[0] + "_last"

producer = KafkaProducer(acks = 0, compression_type = 'gzip',
                         bootstrap_servers = args.broker,
                         value_serializer = lambda x: dumps(x).encode('utf-8'))

last = 0

def log_exit():
    with open(last_path, "w") as last_file:
        last_file.write(str(last))
    print("%d 위치 저장 완료"%(last))

try:
    with open(last_path, "r") as last_file:
        last = int(last_file.readline())
except FileNotFoundError:
    last = 0

atexit.register(log_exit)
with open(log_path, "r") as file:
    if last:
        file.seek(last)
    
    while True:
        where = file.tell()
        line = file.readline()
        if not line:
            time.sleep(1)
            file.seek(where)
        else:
            print(line)
            producer.send(args.topic[0], value = line)
            producer.flush()
            last = file.tell()
