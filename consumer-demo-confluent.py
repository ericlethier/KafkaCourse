from confluent_kafka import Consumer, KafkaError
from signal import signal, SIGINT
from sys import exit

kafka_consumer = None

def handler(signal_received, frame):
    # Handle any cleanup here
    print('SIGINT or CTRL-C detected. Closing kafka_consumer')
    kafka_consumer.close()
    exit(0)


def connect_kafka_consumer(topic):
    kafka_consumer = Consumer({
    'bootstrap.servers': 'localhost:9092',
    'group.id': '0',
    'auto.offset.reset': 'earliest'
    })
    kafka_consumer.subscribe([topic])
    return kafka_consumer

def consume_data(kafka_consumer):
    signal(SIGINT, handler)
    while True:
        msg = kafka_consumer.poll(100)

        if msg is None:
            continue
        if msg.error():
            print("Consumer error: {}".format(msg.error()))
            continue

        print('Received message: {}'.format(msg.value().decode('utf-8')))


if __name__ == '__main__':
    kafka_consumer = connect_kafka_consumer('test_topic')
    consume_data(kafka_consumer)
