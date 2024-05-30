package com.beam.project.demo.kafka;

import com.beam.project.common.KafkaOptions;
import org.apache.beam.sdk.options.PipelineOptionsFactory;
import org.joda.time.Duration;
import org.joda.time.Instant;

import java.util.Timer;
import java.util.TimerTask;

public class ExamScorePipeline {

    public static void main(String[] args) {
        KafkaOptions options = PipelineOptionsFactory.fromArgs(args).withValidation().as(KafkaOptions.class);
        options.setKafkaHost("data-master01:9092,data-worker01:9092,data-worker02:9092");
        /*
         * Kafka producer which sends messages (works in background thread)
         */
        Duration windowSize = Duration.standardSeconds(30);
        Instant nextWindowStart =new Instant(Instant.now().getMillis() + windowSize.getMillis()
                                - Instant.now().plus(windowSize).getMillis() % windowSize.getMillis());
        ExamScoreProducer producer = new ExamScoreProducer(options);
        Timer timer = new Timer();
        TimerTask task = new TimerTask() {
            @Override
            public void run() {
                producer.runPipeline();
            }
        };
        timer.schedule(task,nextWindowStart.toDate());

        /*
         * Kafka consumer which reads messages
         */
        ExamScoreConsumer kafkaConsumer = new ExamScoreConsumer(options);
        kafkaConsumer.run();
    }

}
