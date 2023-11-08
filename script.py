from confluent_kafka import Producer, Consumer, KafkaError


class KafkaClient:
    """
        A simple Kafka client for producing and consuming messages using the confluent-kafka-python library.

        Parameters:
        - bootstrap_servers (str): The Kafka broker address in the format 'hostname:port'.
        - group_id (str): The consumer group ID for the Kafka consumer.
        - auto_offset_reset (str, optional): The initial offset for the Kafka consumer. Default is 'earliest'.

        Attributes:
        - bootstrap_servers (str): The Kafka broker address.
        - group_id (str): The consumer group ID.
        - auto_offset_reset (str): The initial offset for the Kafka consumer.
        - producer (confluent_kafka.Producer): The Kafka producer instance.
        - consumer (confluent_kafka.Consumer): The Kafka consumer instance.

        Methods:
        - create_producer(): Create a Kafka producer instance.
        - create_consumer(topics): Create a Kafka consumer instance and subscribe to the specified topics.
        - produce_message(topic, key, value): Produce a message to the specified topic with the given key and value.
        - consume_messages(): Consume messages from the subscribed topics and print them.
        - close(): Close the Kafka producer and consumer instances.

        """
    def __init__(self, bootstrap_servers, group_id, auto_offset_reset='earliest'):
        self.bootstrap_servers = bootstrap_servers
        self.group_id = group_id
        self.auto_offset_reset = auto_offset_reset
        self.producer = None
        self.consumer = None

    def create_producer(self):
        """
       Create a Kafka producer instance.

       Returns:
       None

       """
        producer_config = {
            'bootstrap.servers': self.bootstrap_servers,
            'client.id': 'python-producer'
        }
        self.producer = Producer(producer_config)

    def create_consumer(self, topics):
        """
        Create a Kafka consumer instance and subscribe to the specified topics.

        Parameters:
        - topics (list): A list of topics to subscribe to.

        Returns:
        None
        """
        consumer_config = {
            'bootstrap.servers': self.bootstrap_servers,
            'group.id': self.group_id,
            'auto.offset.reset': self.auto_offset_reset
        }
        self.consumer = Consumer(consumer_config)
        self.consumer.subscribe(topics)

    def produce_message(self, topic, key, value):
        """
       Produce a message to the specified topic with the given key and value.

       Parameters:
       - topic (str): The Kafka topic to produce the message to.
       - key: The key for the message.
       - value: The value of the message.

       Returns:
       None
       """
        if self.producer:
            self.producer.produce(topic, key=key, value=value)
            self.producer.flush()

    def consume_messages(self):
        """
        Consume messages from the subscribed topics and print them.

        Returns:
        None
        """
        if self.consumer:
            while True:
                msg = self.consumer.poll(1.0)  # Poll for messages with a timeout
                if msg is None:
                    continue
                if msg.error():
                    if msg.error().code() == KafkaError._PARTITION_EOF:
                        print("Reached end of partition")
                    else:
                        print("Error while consuming: {}".format(msg.error()))
                else:
                    print('Received message: key={}, value={}'.format(msg.key(), msg.value()))

    def close(self):
        """
        Close the Kafka producer and consumer instances.

        Returns:
        None
        """
        if self.producer:
            self.producer.close()
        if self.consumer:
            self.consumer.close()


if __name__ == '__main__':
    # Define Kafka broker address and topic
    bootstrap_servers = 'your_kafka_broker_address:9092'
    topic_name = 'your_topic_name'

    # Create a KafkaClient instance
    kafka_client = KafkaClient(bootstrap_servers, 'python-consumer-group')

    # Create a Kafka producer and send a message
    kafka_client.create_producer()
    kafka_client.produce_message(topic_name, 'key1', 'Hello, Kafka!')

    # Create a Kafka consumer and consume messages
    kafka_client.create_consumer([topic_name])
    kafka_client.consume_messages()

    # Close the Kafka client
    kafka_client.close()
