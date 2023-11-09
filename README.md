# KafkaClient

KafkaClient is a Python class that encapsulates Kafka producer and consumer functionality using the confluent-kafka library. This class provides a convenient way to create a Kafka producer and consumer, produce and consume messages, and close the Kafka client.

## Usage

1. Create an instance of KafkaClient with the Kafka broker address and group ID.
2. Use `create_producer()` to create a Kafka producer.
3. Use `produce_message()` to send messages to a specified Kafka topic.
4. Use `create_consumer()` to create a Kafka consumer and subscribe to a topic.
5. Use `consume_messages()` to start consuming messages.
6. Use `close()` to gracefully close the Kafka client.

## Example

```python
if __name__ == '__main__':
    # Define Kafka broker address and topic
    bootstrap_servers = 'your_kafka_broker_address:9092'
    topic_name = 'your_topic_name'

    # Create a KafkaClient instance
    kafka_client = KafkaClient(bootstrap_servers, 'python-consumer-group')

    # Create a Kafka producer and send a message
    kafka_client create_producer()
    kafka_client produce_message(topic_name, 'key1', 'Hello, Kafka!')

    # Create a Kafka consumer and consume messages
    kafka_client create_consumer([topic_name])
    kafka_client consume_messages()

    # Close the Kafka client
    kafka_client close()
```
## Author

- Author: Yasir Ali
- LinkedIn: [Yasir Ali on LinkedIn](https://www.linkedin.com/in/yasirali179/)

## Class Methods

### `__init__(self, bootstrap_servers, group_id, auto_offset_reset='earliest')`
- Constructor for the KafkaClient class.
- **Parameters:**
  - `bootstrap_servers (str)`: The Kafka broker address in the format 'hostname:port'.
  - `group_id (str)`: The consumer group ID for the Kafka consumer.
  - `auto_offset_reset (str, optional)`: The initial offset for the Kafka consumer. Default is 'earliest'.

### `create_producer(self)`
- Create a Kafka producer instance.

### `create_consumer(self, topics)`
- Create a Kafka consumer instance and subscribe to the specified topics.
- **Parameters:**
  - `topics (list)`: A list of topics to subscribe to.

### `produce_message(self, topic, key, value)`
- Produce a message to the specified topic with the given key and value.
- **Parameters:**
  - `topic (str)`: The Kafka topic to produce the message to.
  - `key`: The key for the message.
  - `value`: The value of the message.

### `consume_messages(self)`
- Consume messages from the subscribed topics and print them.

### `close(self)`
- Close the Kafka producer and consumer instances.