package com.beam.project.demo.stream;

import com.beam.project.common.KafkaOptions;
import org.apache.beam.runners.flink.FlinkRunner;
import org.apache.beam.sdk.options.PipelineOptionsFactory;

/**
 * com.beam.project.demo.stream.ScoreProcessPipeline
 * --runner=FlinkRunner
 */
public class ScoreProcessPipeline {

    public static void main(String[] args) {

        KafkaOptions options = PipelineOptionsFactory.fromArgs(args).withValidation().as(KafkaOptions.class);
        options.setTopic("SchoolClassTopic");
        options.setRunner(FlinkRunner.class);
        /*
         * Kafka producer which sends messages (works in background thread)
         */
/*        Duration windowSize = Duration.standardSeconds(KafkaOptions.WINDOW_TIME);
        Instant nowInstant = Instant.now();
        Instant nextWindowStart = new Instant(nowInstant.getMillis() + windowSize.getMillis()
                - nowInstant.plus(windowSize).getMillis() % windowSize.getMillis());
        MessageProducer producer = new MessageProducer(options);
        Timer timer = new Timer();
        TimerTask task = new TimerTask() {
            @Override
            public void run() {
                producer.runPipeline();
            }
        };
        timer.schedule(task, nextWindowStart.toDate());*/

        /*
         * Kafka consumer which reads messages
         */
        MessageConsumer kafkaConsumer = new MessageConsumer(options);
        kafkaConsumer.runPipeline();
    }

}
