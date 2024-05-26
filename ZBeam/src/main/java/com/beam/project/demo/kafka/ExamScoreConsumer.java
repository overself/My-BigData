package com.beam.project.demo.kafka;

import avro.shaded.com.google.common.collect.ImmutableMap;
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
import org.apache.beam.sdk.values.PCollection;
import org.apache.kafka.common.serialization.StringDeserializer;

@Slf4j
public class ExamScoreConsumer {

    private KafkaOptions options;

    private ObjectMapper objectMapper = new ObjectMapper();

    public ExamScoreConsumer(KafkaOptions options) {
        this.options = options;
    }

    public void run() {

        Pipeline pipeline = Pipeline.create(options);
        PCollection<KafkaRecord<String, String>> kafkaRecords = pipeline.apply("ReadFromKafka",
                KafkaIO.<String, String>read()
                        .withBootstrapServers(options.getKafkaHost())
                        .withTopic(options.getTopic())
                        .withKeyDeserializer(StringDeserializer.class)
                        .withValueDeserializer(StringDeserializer.class)
                        .withConsumerConfigUpdates(ImmutableMap.of("group.id", "beam_score_1"))
                        .withReadCommitted());
        //PCollection<ExamScore> scorePc = kafkaRecords.apply(ParDo.of(new ConvertExamScoreDoFn()));
        PCollection<ExamScore> scorePc = kafkaRecords.apply(MapElements.via(new ConvertExamScoreFunction()));

        scorePc.apply(ParDo.of(new LogOutput<>("接收成绩")));
        pipeline.run().waitUntilFinish();

    }

    private static class ConvertExamScoreDoFn extends DoFn<KafkaRecord<String, String>, ExamScore> {
        @DoFn.ProcessElement
        public void processElement(ProcessContext context) {
            KafkaRecord<String, String> input = context.element();
            ObjectMapper objectMapper = new ObjectMapper();
            ExamScore examScore = objectMapper.convertValue(input.getKV().getValue(), ExamScore.class);
            context.output(examScore);
        }
    }

    private static class ConvertExamScoreFunction extends SimpleFunction<KafkaRecord<String, String>, ExamScore> {
        @Override
        public ExamScore apply(KafkaRecord<String, String> input) {
            ObjectMapper objectMapper = new ObjectMapper();
            return objectMapper.convertValue(input.getKV().getValue(), ExamScore.class);
        }
    }
}
