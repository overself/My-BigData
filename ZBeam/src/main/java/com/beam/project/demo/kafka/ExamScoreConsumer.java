package com.beam.project.demo.kafka;

import avro.shaded.com.google.common.collect.ImmutableMap;
import com.beam.project.common.KafkaOptions;
import com.beam.project.common.LogOutput;
import com.beam.project.demo.bean.ExamScore;
import com.fasterxml.jackson.databind.ObjectMapper;
import lombok.extern.slf4j.Slf4j;
import org.apache.beam.sdk.Pipeline;
import org.apache.beam.sdk.io.kafka.KafkaIO;
import org.apache.beam.sdk.io.kafka.KafkaRecord;
import org.apache.beam.sdk.transforms.DoFn;
import org.apache.beam.sdk.transforms.MapElements;
import org.apache.beam.sdk.transforms.ParDo;
import org.apache.beam.sdk.transforms.SimpleFunction;
import org.apache.beam.sdk.transforms.windowing.*;
import org.apache.beam.sdk.values.KV;
import org.apache.beam.sdk.values.PCollection;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.joda.time.Duration;

@Slf4j
public class ExamScoreConsumer {

    private final KafkaOptions options;

    public ExamScoreConsumer(KafkaOptions options) {
        this.options = options;
    }

    public void run() {

        Pipeline pipeline = Pipeline.create(options);
        PCollection<KV<String, String>> kafkaMessage = pipeline.apply("ReadFromKafka",
                KafkaIO.<String, String>read()
                        .withBootstrapServers(options.getKafkaHost())
                        .withTopic(options.getTopic())
                        .withKeyDeserializer(StringDeserializer.class)
                        .withValueDeserializer(StringDeserializer.class)
                        .withConsumerConfigUpdates(ImmutableMap.of("group.id", "beam_score_1"))
                        .withReadCommitted().withoutMetadata());

        // Create fixed window for the length of the game round
        Window<KV<String, String>> window = Window.into(FixedWindows.of(Duration.standardSeconds(5)));
        Trigger trigger = AfterProcessingTime.pastFirstElementInPane()
                .plusDelayOf(Duration.standardSeconds(4));

        PCollection<KV<String, String>> records = kafkaMessage.apply(window.triggering(Repeatedly.forever(trigger))
                .withAllowedLateness(Duration.standardSeconds(2))
                .accumulatingFiredPanes());
        records.apply(ParDo.of(new LogOutput<>("接收records")));

        //PCollection<ExamScore> scorePc = records.apply(ParDo.of(new ConvertExamScoreDoFn()));
        //PCollection<ExamScore> scorePc = records.apply(MapElements.via(new ConvertExamScoreFunction()));

        //scorePc.apply(ParDo.of(new LogOutput<>("接收成绩")));
        pipeline.run().waitUntilFinish();

    }

    private static class ConvertExamScoreDoFn extends DoFn<KV<String, String>, ExamScore> {
        @ProcessElement
        public void processElement(ProcessContext context) {
            KV<String, String> input = context.element();
            ObjectMapper objectMapper = new ObjectMapper();
            ExamScore examScore = objectMapper.convertValue(input.getValue(), ExamScore.class);
            context.output(examScore);
        }
    }

    private static class ConvertExamScoreFunction extends SimpleFunction<KV<String, String>, ExamScore> {
        @Override
        public ExamScore apply(KV<String, String> input) {
            ObjectMapper objectMapper = new ObjectMapper();
            return objectMapper.convertValue(input.getValue(), ExamScore.class);
        }
    }
}
