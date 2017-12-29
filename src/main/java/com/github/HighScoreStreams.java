package com.github;

import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.streams.*;
import org.apache.kafka.streams.kstream.KStream;
import org.apache.kafka.streams.kstream.KTable;
import org.apache.kafka.streams.kstream.Produced;
import org.apache.kafka.streams.processor.FailOnInvalidTimestamp;

import java.util.Properties;
import java.util.concurrent.TimeUnit;

/**
 * This class encapsulates Kafka Streams topology
 * It constantly reads data from 'scores' topic and if a user had a high score it sends it to 'high-scores' topic
 * It is possible that 'high-scores' topic contains duplicate key for a specific user if and only if the user has hit the record
 * Because 'high-scores' cleanup policy is 'compact', Kafka automatically purge the old key
 */
public class HighScoreStreams extends Thread {

    private KafkaStreams streams;

    @Override
    public void run() {

        final StreamsBuilder builder = new StreamsBuilder();

        KStream<String, Integer> scores = builder.stream(Constants.SCORES_TOPIC_NAME, Consumed.with(Serdes.String(), Serdes.Integer()));
        KTable<String, Integer> highScores = builder.table(Constants.HIGH_SCORES_TOPIC_NAME, Consumed.with(Serdes.String(), Serdes.Integer(),new FailOnInvalidTimestamp(), Topology.AutoOffsetReset.EARLIEST));

        scores.leftJoin( highScores , (v1,v2) -> {

            //this is the first time user has submitted his/her score, so there is no record for this user in 'high-scores' topic
            if(v2 == null){
                return v1;
            }

            //user has hit his/hre record
            if( v1 > v2 ) {
                return v1;
            }

            //this value is not a record, we return null but we don't send null values to 'high-scores' topic
            return null;

        })
        .filter( (k,v) -> v != null ) //filter null values
        .to( "high-scores" , Produced.with(Serdes.String(),Serdes.Integer()));

        Topology topology = builder.build();
        Properties props = new Properties();
        props.setProperty(StreamsConfig.APPLICATION_ID_CONFIG,"high-score-app");
        props.setProperty(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, Constants.KAFKA_BROKER);
        props.setProperty(StreamsConfig.COMMIT_INTERVAL_MS_CONFIG,"10000");
        streams = new KafkaStreams( topology , props );
        streams.cleanUp(); //TODO remove this line in a production environment

        Runtime.getRuntime().addShutdownHook(new Thread(){
            @Override
            public void run() {
                streams.close();
            }
        });
        streams.start();
    }

    public void stopStreaming(){
        streams.close(10000, TimeUnit.MILLISECONDS);
    }
}
