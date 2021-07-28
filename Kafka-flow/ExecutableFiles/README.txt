Kafka Flows

#Below command is used to check the list of Kafka topic int the cluster
/Applications/kafka/kafka/bin/kafka-topics.sh --list --zookeeper localhost:2181

#Below Command is used to bring up Kafka Server after zookeeper
/Applications/kafka/kafka/bin/kafka-server-start.sh /Applications/kafka/kafka/config/server.properties

#Below Command is used to bring up zookeeper
/Applications/kafka/kafka/bin/zookeeper-server-start.sh /Applications/kafka/kafka/config/zookeeper.properties

#Bring up Kafka Producer from CLI
/Applications/kafka/kafka/bin/kafka-console-producer.sh --broker-list localhost:9092 --topic testTopic1

#Create Topic in Kafka Server
/Applications/kafka/kafka/bin/kafka-topics.sh --create --zookeeper localhost:2181 --replication-factor 1 --partitions 1 --topic testTopic1

#Kafka COnsole Consumer from CLI
/Applications/kafka/kafka/bin/kafka-console-consumer.sh --bootstrap-server localhost:9092 --topic testTopic1 --from-beginning