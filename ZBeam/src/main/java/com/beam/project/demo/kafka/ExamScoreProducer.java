package com.beam.project.demo.kafka;

import avro.shaded.com.google.common.collect.ImmutableMap;
import com.beam.project.common.KafkaOptions;
import com.beam.project.common.PipelineRunner;
import com.beam.project.demo.bean.ExamScore;
import com.beam.project.demo.bean.Subject;
import com.beam.project.demo.kafka.transform.RandomScoreGeneratorFn;
import com.fasterxml.jackson.databind.ObjectMapper;
import lombok.SneakyThrows;
import org.apache.beam.sdk.Pipeline;
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

    private KafkaOptions options;

    public ExamScoreProducer(KafkaOptions options) {
        this.options = options;
    }

    @Override
    public void runPipeline() {

        Pipeline pipeline = Pipeline.create(options);

        GenerateSequence sequence = GenerateSequence.from(0)
                .withRate(KafkaOptions.MESSAGES_COUNT, Duration.standardSeconds(KafkaOptions.WINDOW_TIME));
        sequence.withTimestampFn((Long n) -> new Instant(System.currentTimeMillis()));

        PCollection<ExamScore> inputScore = pipeline
                .apply(sequence).apply(ParDo.of(new RandomScoreGeneratorFn(Subject.mathematics)));
        //inputScore.apply(ParDo.of(new LogOutput<>("ExamScoreData")));

        PCollection<KV<String, String>> JsonDataPc = inputScore.apply(ParDo.of(new ConvertExamScoreToJson()));
        //JsonDataPc.apply(ParDo.of(new LogOutput<>("ExamScoreJSON")));

        KafkaIO.Write<String, String> kafkaIo = KafkaIO.<String, String>write()
                .withBootstrapServers(options.getKafkaHost())
                .withTopic(options.getTopic())
                .withKeySerializer(StringSerializer.class)
                .withValueSerializer(StringSerializer.class)
                .withProducerConfigUpdates(ImmutableMap.of("group.id", "beam_score_1"));
        JsonDataPc.apply(kafkaIo);
        pipeline.run().waitUntilFinish();
    }

    private static class ConvertExamScoreToJson extends DoFn<ExamScore, KV<String, String>> {
        @SneakyThrows
        @ProcessElement
        public void process(@Element ExamScore element, OutputReceiver<KV<String, String>> receiver){
            ObjectMapper objectMapper = new ObjectMapper();
            receiver.output(KV.of(element.getStudentCode() + element.getSubject().getValue(),
                    objectMapper.writeValueAsString(element)));
        }
    }
}
