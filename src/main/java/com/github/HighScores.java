package com.github;

import com.github.model.Score;
import org.apache.kafka.clients.consumer.*;

import java.util.*;
import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.BlockingQueue;

/**
 * HighScores read all data from 'high-scores' table and selects top 5 high scores
 *
 */
public class HighScores implements Iterable<List<Score>> {

    private final int CAPACITY = 5; //only top 5
    private List<Score> topHighScores;
    private BlockingQueue<List<Score>> blockingQueue = new ArrayBlockingQueue<>(1);

    public HighScores() {
        topHighScores = new ArrayList<>();
    }

    private boolean tryAdd(Score score) {
        if( topHighScores.size() == CAPACITY ) {

            if( score.getScore() >= topHighScores.get(0).getScore() ) {
                topHighScores.remove(0);
                topHighScores.add( score );
                Collections.sort( topHighScores );
                return true;
            }

            return false;

        } else {
            topHighScores.add(score);
            Collections.sort( topHighScores  );
            return true;
        }
    }

    @Override
    public Iterator<List<Score>> iterator() {
        return new HighScoresIterator();
    }

    private class HighScoresIterator implements Iterator<List<Score>> {

        public HighScoresIterator() {

            new Thread(() -> {

                Properties props = new Properties();
                props.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG , Constants.KAFKA_BROKER);
                props.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG , "earliest"); //because we need all data
                props.put(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG , "false");
                props.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG,"org.apache.kafka.common.serialization.StringDeserializer");
                props.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG,"org.apache.kafka.common.serialization.IntegerDeserializer");
                props.put(ConsumerConfig.GROUP_ID_CONFIG , "g" + UUID.randomUUID());
                Consumer<String,Integer> consumer = new KafkaConsumer<>(props);
                consumer.subscribe(Collections.singleton(Constants.HIGH_SCORES_TOPIC_NAME));

                while (true){
                    ConsumerRecords<String, Integer> records = consumer.poll(2000);
                    for (ConsumerRecord<String, Integer> record : records) {
                        boolean highScore = tryAdd( new Score(record.key() , record.value() , record.timestamp()) );
                        if( highScore ) {
                            try {
                                blockingQueue.put( topHighScores );
                            } catch (InterruptedException e) {
                                e.printStackTrace();
                            }
                        }
                    }
                }

            }).start();

        }

        @Override
        public boolean hasNext() {
            return true;
        }

        @Override
        public List<Score> next() {
            try {
                return new ArrayList<>(blockingQueue.take());
            } catch (InterruptedException e) {
                throw new RuntimeException();
            }
        }
    }
}
