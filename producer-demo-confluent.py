from confluent_kafka import Producer


def on_completion(error, msg):
    if error is not None:
        print('Message delivery failed: {}'.format(error))
    else:
        print('Message delivered to {} partition: [{}] offset: {}'.format(msg.topic(), msg.partition(), msg.offset()))

def publish_keyed_message_callback(kafka_producer, topic_name):
    try:
        for id in range(10):
            key= "key_" + str(id)
            value = "value_" + str(id) 
            kafka_producer.produce(topic_name, key=key.encode('utf-8'), value=value.encode('utf-8'), callback=on_completion)
            kafka_producer.flush()
            print('Message published successfully.')
    except Exception as ex:
        print('Exception in publishing message')
        print(str(ex))

def connect_kafka_producer():
    try:
        kafka_producer = Producer({'bootstrap.servers':'localhost:9092'})
    except Exception as ex:
        print('Exception while connecting Kafka')
        print(str(ex))
    finally:
        return kafka_producer

if __name__ == '__main__':
    kafka_producer = connect_kafka_producer()
    publish_keyed_message_callback(kafka_producer, 'test_topic')
