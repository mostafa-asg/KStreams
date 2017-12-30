![project screenshot](asset/KafkaStreams.png)
### How to run
1. Start kafka cluster.
2. Create topics based on [this](https://mostafa-asg.github.io/posts/reactive-kafka-streams-sse-top5/) article
3. mvn clean package
4. java -jar target/Top5KafkaStreams-1.0-SNAPSHOT-jar-with-dependencies.jar
5. navigate to localhost:4567

**Tip:** Kafka cluster address is hardcoded on Constants.Java. If your cluster address is other than localhost:9092, change it.
