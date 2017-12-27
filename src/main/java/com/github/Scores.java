package com.github;

import com.github.model.Score;
import org.apache.kafka.clients.consumer.*;
import org.apache.kafka.common.TopicPartition;

import java.util.*;
import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;

import static com.github.Constants.*;

/**
 * Iterable implementation over 'scores' topic
 */
public class Scores implements Iterable<Score> {

    @Override
    public Iterator<Score> iterator() {
        return new ScoresIterator();
    }

    private class ScoresIterator implements Iterator<Score> {

        final private ArrayBlockingQueue<Score> blockingQueue = new ArrayBlockingQueue<>(100);
        private ConsumerRecords<String,Integer> lastFetchedRecords;
        private ExecutorService executorService = Executors.newCachedThreadPool();

        public ScoresIterator() {

            new Thread( () -> {

                TopicPartition topicPartition = new TopicPartition(SCORES_TOPIC_NAME,0);

                Properties props = new Properties();
                props.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG , Constants.KAFKA_BROKER);
                props.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG , "earliest");
                props.put(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG , "false");
                props.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG,"org.apache.kafka.common.serialization.StringDeserializer");
                props.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG,"org.apache.kafka.common.serialization.IntegerDeserializer");
                props.put(ConsumerConfig.GROUP_ID_CONFIG , "g" + UUID.randomUUID());
                KafkaConsumer kafkaConsumer = new KafkaConsumer<>(props);
                kafkaConsumer.assign( Collections.singleton(topicPartition) );

                //Start from endOffset minus 10
                Map<TopicPartition,Long> offsets = kafkaConsumer.endOffsets( Collections.singleton(topicPartition) );
                long endOffset = offsets.get(topicPartition);
                long beginOffset = endOffset > 10 ? endOffset-10 : endOffset;
                kafkaConsumer.seek( topicPartition , beginOffset );

                Future<?> future = null;

                while (true) {

                    ConsumerRecords<String, Integer> records = kafkaConsumer.poll(2000);

                    if( !records.isEmpty() ) {
                        lastFetchedRecords = records;
                        kafkaConsumer.pause( Collections.singleton( new TopicPartition(SCORES_TOPIC_NAME,0)) );

                        future = executorService.submit( () -> {
                            for (ConsumerRecord<String, Integer> record : lastFetchedRecords) {
                                try {
                                    blockingQueue.put( new Score( record.key() , record.value() , record.timestamp() ) );
                                } catch (InterruptedException e) {
                                    e.printStackTrace();
                                }
                            }

                            lastFetchedRecords = null;
                        });
                    }

                    if( future != null && future.isDone() ){
                        kafkaConsumer.resume( Collections.singleton( new TopicPartition(SCORES_TOPIC_NAME,0)) );
                    }
                }
            }).start();
        }

        @Override
        public boolean hasNext() {
            // hasNext is always returns true because we dealing with streams of events
            // and by definition streams is infinite of events
            return true;
        }

        @Override
        public Score next() {
            try {
                return blockingQueue.take();
            } catch (InterruptedException e) {
                throw new RuntimeException();
            }
        }

    }
}
