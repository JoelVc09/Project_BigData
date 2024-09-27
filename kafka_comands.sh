
# start zookeeper
cd /mnt/c/kafka_2.13-3.8.0/kafka_2.13-3.8.0
bin/zookeeper-server-start.sh config/zookeeper.properties

# start kafka broker
cd /mnt/c/kafka_2.13-3.8.0/kafka_2.13-3.8.0
bin/kafka-server-start.sh config/server.properties

# create topic
cd /mnt/c/kafka_2.13-3.8.0/kafka_2.13-3.8.0
bin/kafka-topics.sh --create --topic water_quality_data --bootstrap-server localhost:9092 --replication-factor 1 --partitions 1

python3 -m venv kafka-env
source kafka-env/bin/activate
pip install kafka-python


# read events from topic
cd /mnt/c/kafka_2.13-3.8.0/kafka_2.13-3.8.0
bin/kafka-console-consumer.sh --topic water_quality_data --bootstrap-server localhost:9092


