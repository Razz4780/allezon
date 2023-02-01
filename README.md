# allezon

## ApiServer
Accepts HTTP requests. Pushes user tags to Kafka and queries Aerospike for data. To build the container, run `docker build -f Dockerfile.api_server .` in the root of the project.

Configuration is passed through environment variables:
1. `address` - address of the socket the server will listen on
2. `kafka_brokers` - a comma-separated list of Kafka instances this app will initially connect to (socket addresses)
3. `kafka_topic` - a topic for user tags in Kafka

## Consumer
Consumer user tags from Kafka and writes to Aerospike. To build the container, run `docker build -f Dockerfile.consumer .` in the root of the project.

Configuration is passed through environment variables:
1. `kafka_brokers` - a comma-separated list of Kafka instances this app will initially connect to (socket addresses)
2. `kafka_group` - a Kafka group of this consumer
3. `kafka_topic` - a topic for user tags in Kafka
