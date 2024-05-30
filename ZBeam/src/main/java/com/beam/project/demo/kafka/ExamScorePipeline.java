package com.beam.project.demo.kafka;

import com.beam.project.common.KafkaOptions;
import org.apache.beam.sdk.options.PipelineOptionsFactory;

import java.util.TimerTask;

public class ExamScorePipeline {

    public static void main(String[] args) {
        KafkaOptions options = PipelineOptionsFactory.fromArgs(args).withValidation().as(KafkaOptions.class);
        options.setKafkaHost("data-master01:9092,data-worker01:9092,data-worker02:9092");
        /*
         * Kafka producer which sends messages (works in background thread)
         */
        ExamScoreProducer producer = new ExamScoreProducer(options);
        TimerTask task = new TimerTask() {
            @Override
            public void run() {
                producer.run();
            }
        };
        task.run();

        /*
         * Kafka consumer which reads messages
         */
        ExamScoreConsumer kafkaConsumer = new ExamScoreConsumer(options);
        kafkaConsumer.run();
    }

}
