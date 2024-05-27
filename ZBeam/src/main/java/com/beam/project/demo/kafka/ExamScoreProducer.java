package com.beam.project.demo.kafka;

import avro.shaded.com.google.common.collect.ImmutableMap;
import com.beam.project.common.LogOutput;
import com.beam.project.demo.bean.ExamScore;
import com.beam.project.demo.bean.Subject;
import com.beam.project.demo.kafka.transform.RandomScoreGeneratorFn;
import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.beam.sdk.Pipeline;
import org.apache.beam.sdk.coders.SerializableCoder;
import org.apache.beam.sdk.io.GenerateSequence;
import org.apache.beam.sdk.io.kafka.KafkaIO;
import org.apache.beam.sdk.transforms.DoFn;
import org.apache.beam.sdk.transforms.ParDo;
import org.apache.beam.sdk.values.KV;
import org.apache.beam.sdk.values.PCollection;
import org.apache.kafka.common.serialization.StringSerializer;
import org.joda.time.Duration;
import org.joda.time.Instant;

public class ExamScoreProducer implements PipelineRunner {

    private static final int MESSAGES_COUNT = 100;

    private static final int WINDOW_TIME = 30;


    private KafkaOptions options;

    public ExamScoreProducer(KafkaOptions options) {
        this.options = options;
    }

    @Override
    public void run() {

        Pipeline pipeline = Pipeline.create(options);

        GenerateSequence sequence = GenerateSequence.from(0)
                .withRate(MESSAGES_COUNT, Duration.standardSeconds(WINDOW_TIME));
        sequence.withTimestampFn((Long n) -> new Instant(System.currentTimeMillis()));

        PCollection<ExamScore> inputScore = pipeline
                .apply(sequence).apply(ParDo.of(new RandomScoreGeneratorFn(Subject.mathematics)));
        PCollection<KV<String, String>> JsonDataPc = inputScore.setCoder(SerializableCoder.of(ExamScore.class))
                .apply(ParDo.of(new ConvertExamScoreToJson()));

        KafkaIO.Write<String, String> kafkaIo = KafkaIO.<String, String>write()
                .withBootstrapServers(options.getKafkaHost())
                .withTopic(options.getTopic())
                .withKeySerializer(StringSerializer.class)
                .withValueSerializer(StringSerializer.class)
                .withProducerConfigUpdates(ImmutableMap.of("group.id", "beam_score_1"));
        JsonDataPc.apply(kafkaIo);

        JsonDataPc.apply(ParDo.of(new LogOutput<>("发送成绩")));
        pipeline.run().waitUntilFinish();
    }

    private static class ConvertExamScoreToJson extends DoFn<ExamScore, KV<String, String>> {
        @ProcessElement
        public void processElement(@DoFn.Element ExamScore element, OutputReceiver<KV<String, String>> receiver) throws JsonProcessingException {
            ObjectMapper objectMapper = new ObjectMapper();
            receiver.output(KV.of(element.getStudentCode() + element.getSubject().getValue(),
                    objectMapper.writeValueAsString(element)));
        }
    }
}