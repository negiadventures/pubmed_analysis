Steps:
1. Download Kafka
https://www.apache.org/dyn/closer.cgi?path=/kafka/3.0.0/kafka_2.13-3.0.0.tgz

2. Start Zookeeper
bin/zookeeper-server-start.sh config/zookeeper.properties

3. Start Broker
bin/kafka-server-start.sh config/server.properties

4. Create topic if does not exists
bin/kafka-topics.sh --create --topic file_links --partitions 3 --replication-factor 1 --bootstrap-server localhost:9092

5. Start consumer app:
python3 kafka_consumer.py
NOTE: You can run multiple instances of consumers/ listeners for parallel processing

6. Send messages through producer app:
python3 kafka_producer.py

IDEA:
- We are implementing kafka streaming where the producer polls on the ftp location for any unprocessed files and sends the message to a kafka topic and the consumers listen to these topics and process individual files.  
- The processing involves reading the xml.gz file, parsing it to csv, data cleaning, reduction and transformation.
- Apart from this, consumers also create csv files for the edge network that is found in the PubMed citation dataset. 
