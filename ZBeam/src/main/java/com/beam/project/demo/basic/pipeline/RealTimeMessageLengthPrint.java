package com.beam.project.demo.basic.pipeline;

import com.beam.project.common.KafkaOptions;
import com.google.common.collect.ImmutableMap;
import lombok.extern.slf4j.Slf4j;
import org.apache.beam.runners.flink.FlinkRunner;
import org.apache.beam.sdk.Pipeline;
import org.apache.beam.sdk.io.kafka.KafkaIO;
import org.apache.beam.sdk.options.PipelineOptionsFactory;
import org.apache.beam.sdk.transforms.DoFn;
import org.apache.beam.sdk.transforms.GroupByKey;
import org.apache.beam.sdk.transforms.ParDo;
import org.apache.beam.sdk.transforms.windowing.*;
import org.apache.beam.sdk.values.KV;
import org.apache.beam.sdk.values.PCollection;
import org.apache.kafka.common.serialization.LongDeserializer;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.joda.time.Duration;

@Slf4j
public class RealTimeMessageLengthPrint {

    public static void main(String[] args) {
        KafkaOptions options = PipelineOptionsFactory.fromArgs(args).withValidation().as(KafkaOptions.class);
        options.setTopic("ScoreProcess");
        options.setRunner(FlinkRunner.class);
        Pipeline pipeline = Pipeline.create(options);

        // KafkaIO source example
        PCollection<KV<Long, String>> kafkaMessages = pipeline.apply("#ConsumerRealTimeMsg",
                KafkaIO.<Long, String>read()
                        .withBootstrapServers(options.getKafkaHost())
                        .withTopic(options.getTopic())
                        .withKeyDeserializer(LongDeserializer.class)
                        .withValueDeserializer(StringDeserializer.class)
                        .withConsumerConfigUpdates(ImmutableMap.of("group.id", "beam_process_1"))
                        .withReadCommitted().withoutMetadata());

        // Define the trigger
        Trigger trigger = Repeatedly.forever(
                AfterFirst.of(
                        AfterProcessingTime.pastFirstElementInPane().plusDelayOf(Duration.standardSeconds(1)),
                        AfterPane.elementCountAtLeast(20)
                )
        );

        // Apply the windowing strategy
        PCollection<KV<Long, String>> windowedRecords = kafkaMessages.apply(
                Window.<KV<Long, String>>into(new GlobalWindows())
                        .triggering(trigger)
                        .discardingFiredPanes()
                        .withAllowedLateness(Duration.ZERO));

        // Example GroupByKey and Combine transformation
        PCollection<KV<Long, Iterable<String>>> groupedRecords = windowedRecords.apply(GroupByKey.create());

        // Example of further processing, e.g., summing values
        PCollection<KV<Long, Long>> summedRecords = groupedRecords.apply(ParDo.of(new DoFn<KV<Long, Iterable<String>>, KV<Long, Long>>() {
            @ProcessElement
            public void processElement(@Element KV<Long, Iterable<String>> element, OutputReceiver<KV<Long, Long>> receiver) {
                long sum = 0;
                for (String value : element.getValue()) {
                    //sum += Long.parseLong(value);
                    sum += value.length();
                }
                receiver.output(KV.of(element.getKey(), sum));
            }
        }));

        // Print the results to the console (or write to a sink)
        summedRecords.apply(ParDo.of(new DoFn<KV<Long, Long>, Void>() {
            @ProcessElement
            public void processElement(@Element KV<Long, Long> element) {
                log.info("Meassage Key: {}, Length: {}", element.getKey(), element.getValue());
            }
        }));

        pipeline.run().waitUntilFinish();
    }
}
