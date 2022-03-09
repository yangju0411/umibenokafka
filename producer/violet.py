import argparse
import atexit

from log_deliverer.producer import Producer

parser = argparse.ArgumentParser()

parser.add_argument("--name", "-n", required = True, help = "name of job", nargs = 1)
parser.add_argument("--path", "-p", required = True, help = "path of log file", nargs = 1)
parser.add_argument("--topic", "-t", required = True, help = "kafka Topic", nargs = 1)
parser.add_argument("--broker", "-b", required = True, help = "kafka Brokers", nargs = "+")

# argument parsing
args = parser.parse_args()
log_path = args.path[0]
last_path = args.name[0] + "_last"
broker = args.broker
topic = args.topic[0]

kafka_producer = Producer(broker, log_path)

last = 0
# 종료 시 실행될 작업 - 마지막으로 읽어낸 줄 번호 저장
def log_exit():
    with open(last_path, "w") as last_file:
        last_file.write(str(kafka_producer.last))

try:
    with open(last_path, "r") as last_file:
        last = int(last_file.readline())
except FileNotFoundError:
    last = 0

atexit.register(log_exit)

kafka_producer.deliver(last, topic)
