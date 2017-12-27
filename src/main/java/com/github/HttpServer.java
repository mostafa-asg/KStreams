package com.github;

import com.github.model.Score;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.Producer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;

import java.io.PrintWriter;
import java.util.*;

import static spark.Spark.*;

/**
 * Spark http server
 * This routes will be registered:
 *  1. "/" -> this is the home page
 *  2. "/newScore" -> send new record to Kafka
 *  3. "/events" -> SSE to send all events to the client
 *  4. "/highScores" -> SSE to send new records to the client
 */
public class HttpServer {

    private Map<String,String> cache = new HashMap<>();
    private Producer<String,Integer> kafkaProducer;
    private HighScoreStreams scoreStreams;

    public void stop() {
        scoreStreams.stopStreaming();
    }

    public void start() {

        kafkaProducer = createKafkaProducer();

        scoreStreams = new HighScoreStreams();
        scoreStreams.start();

        get("/", (req, res) -> {

            String page = cache.get("/");
            if (page != null) {
                return page;
            }


            page = new String(ResourceUtil.getResource("home.html"));
            cache.put("/", page);

            return page;
        });

        post("newScore", (req, res) -> {

            HttpPostBodyParser parser = new HttpPostBodyParser(req.body());
            String user = parser.get("user");
            int score = Integer.parseInt(parser.get("score"));

            kafkaProducer.send(new ProducerRecord<>(Constants.SCORES_TOPIC_NAME, user, score), (rm, exc) -> {
                if (exc != null) {
                    System.out.println("Error sending message to kafka : " + exc.getMessage());
                }
            });

            res.redirect("/");
            return null;
        });

        get("/events", (req, res) -> {

            res.header("Content-Type", "text/event-stream");
            res.header("Cache-Control", "no-cache");
            PrintWriter writer = new PrintWriter(res.raw().getOutputStream());

            Scores scores = new Scores();
            Iterator<Score> iterator = scores.iterator();
            while (iterator.hasNext()) {
                Score score = iterator.next();

                writer.write("data: " + score.toJson() + "\n\n");
                writer.flush();
            }

            return "";
        });

        get("/highScores", (req, res) -> {

            res.header("Content-Type", "text/event-stream");
            res.header("Cache-Control", "no-cache");
            PrintWriter writer = new PrintWriter(res.raw().getOutputStream());

            HighScores highScores = new HighScores();
            Iterator<List<Score>> iterator = highScores.iterator();
            while (iterator.hasNext()) {
                List<Score> scores = iterator.next();

                writer.write("data: " + toJson(scores) + "\n\n");
                writer.flush();
            }

            return "";
        });
    }

    private String toJson(List<Score> scores) {
        StringBuilder sb = new StringBuilder();
        sb.append("[");
        int i = 0;
        for (Score score : scores) {

            if( i>0 ){
                sb.append( "," );
            }

            sb.append( score.toJson() );
            i++;
        }
        sb.append("]");
        return sb.toString();
    }

    private Producer<String,Integer> createKafkaProducer(){
        Properties kafkaProps = new Properties();
        kafkaProps.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG,"localhost:9092"); //TODO
        kafkaProps.put(ProducerConfig.ACKS_CONFIG, "-1" );
        kafkaProps.put(ProducerConfig.RETRIES_CONFIG, "3" );
        kafkaProps.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, "org.apache.kafka.common.serialization.StringSerializer" );
        kafkaProps.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, "org.apache.kafka.common.serialization.IntegerSerializer" );

        Producer<String,Integer> kafkaProducer = new KafkaProducer<>(kafkaProps);
        return kafkaProducer;
    }
}
