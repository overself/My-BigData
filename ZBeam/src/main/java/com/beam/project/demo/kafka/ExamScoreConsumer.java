package com.beam.project.demo.kafka;

import com.beam.project.common.KafkaOptions;
import com.beam.project.common.LogOutput;
import com.beam.project.common.PipelineRunner;
import com.beam.project.demo.bean.ExamScore;
import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.common.collect.ImmutableMap;
import lombok.SneakyThrows;
import lombok.extern.slf4j.Slf4j;
import org.apache.beam.sdk.Pipeline;
import org.apache.beam.sdk.PipelineResult;
import org.apache.beam.sdk.io.kafka.KafkaIO;
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
public class ExamScoreConsumer implements PipelineRunner {

    private final KafkaOptions options;

    public ExamScoreConsumer(KafkaOptions options) {
        this.options = options;
    }

    public PipelineResult.State runPipeline() {

        Pipeline pipeline = Pipeline.create(options);
        PCollection<KV<String, String>> kafkaMessage = pipeline.apply("ReadFromKafka",
                KafkaIO.<String, String>read()
                        .withBootstrapServers(options.getKafkaHost())
                        .withTopic(options.getTopic())
                        .withKeyDeserializer(StringDeserializer.class)
                        .withValueDeserializer(StringDeserializer.class)
                        .withConsumerConfigUpdates(ImmutableMap.of("group.id", "beam_score_1"))
                        //.withMaxNumRecords(100)
                        .withReadCommitted().withoutMetadata());
        kafkaMessage.apply(ParDo.of(new LogOutput<>("接收到消息")));

        // Create fixed window for the length of the game round
        Window<KV<String, String>> window = Window.into(FixedWindows.of(Duration.standardSeconds(KafkaOptions.WINDOW_TIME)));
        Trigger trigger = AfterProcessingTime.pastFirstElementInPane().plusDelayOf(Duration.standardSeconds(10));

        PCollection<KV<String, String>> records = kafkaMessage.apply(window.triggering(Repeatedly.forever(trigger))
                .withAllowedLateness(Duration.standardSeconds(1))
                .accumulatingFiredPanes());
        kafkaMessage.apply(ParDo.of(new LogOutput<>("Window消息")));

        //PCollection<ExamScore> scorePc = records.apply(ParDo.of(new ConvertExamScoreDoFn()));
        PCollection<ExamScore> scorePc = records.apply(MapElements.via(new ConvertExamScoreFunction()));
        scorePc.apply(ParDo.of(new LogOutput<>("转换至成绩")));

        return pipeline.run().waitUntilFinish();
    }

    private static class ConvertExamScoreDoFn extends DoFn<KV<String, String>, ExamScore> {
        @SneakyThrows
        @ProcessElement
        public void processElement(ProcessContext context) {
            KV<String, String> input = context.element();
            ObjectMapper objectMapper = new ObjectMapper();
            ExamScore examScore = objectMapper.readValue(input.getValue(), new TypeReference<ExamScore>() {
            });
            context.output(examScore);
        }
    }

    private static class ConvertExamScoreFunction extends SimpleFunction<KV<String, String>, ExamScore> {
        @SneakyThrows
        @Override
        public ExamScore apply(KV<String, String> input) {
            ObjectMapper objectMapper = new ObjectMapper();
            return objectMapper.readValue(input.getValue(), new TypeReference<ExamScore>() {
            });
        }
    }
}
