# allezon

# Installation
Run `sudo install.sh [ansible_user] [ansible_password]` on one of the VMs.

## ApiServer
Accepts HTTP requests. Pushes user tags to Kafka and queries Aerospike for data.

Configuration is passed through environment variables:
1. `address` - address of the socket the server will listen on
2. `kafka_brokers` - a comma-separated list of Kafka instances this app will initially connect to (socket addresses)
3. `kafka_topic` - a topic for user tags in Kafka
4. `aerospike_user_profiles` - address of the aerospike server that holds user profiles
5. `aerospike_aggregates` - a comma-separated list of 4 aerospike servers that hold aggregates

## Consumer
Consumer uses tags from Kafka and writes to Aerospike.

Configuration is passed through environment variables:
1. `kafka_brokers` - a comma-separated list of Kafka instances this app will initially connect to (socket addresses)
2. `kafka_group` - a Kafka group of this consumer. When built with `aggregates` feature, group should end with `_{0|1|2|3}`
3. `kafka_topic` - a topic for user tags in Kafka
4. `aerospike` - address of local aerospike server
