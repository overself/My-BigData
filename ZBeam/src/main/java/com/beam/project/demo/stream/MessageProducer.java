package com.beam.project.demo.stream;

import avro.shaded.com.google.common.collect.ImmutableMap;
import com.beam.project.common.KafkaOptions;
import com.beam.project.common.PipelineRunner;
import com.beam.project.core.convert.ObjectMapperJson;
import com.beam.project.demo.bean.SchoolClass;
import com.beam.project.demo.stream.transform.GeneratorSchoolClassDoFn;
import org.apache.beam.sdk.Pipeline;
import org.apache.beam.sdk.PipelineResult;
import org.apache.beam.sdk.io.GenerateSequence;
import org.apache.beam.sdk.io.kafka.KafkaIO;
import org.apache.beam.sdk.transforms.ParDo;
import org.apache.beam.sdk.values.KV;
import org.apache.beam.sdk.values.PCollection;
import org.apache.kafka.common.serialization.LongSerializer;
import org.apache.kafka.common.serialization.StringSerializer;
import org.joda.time.Duration;
import org.joda.time.Instant;

public class MessageProducer implements PipelineRunner {

    private KafkaOptions options;

    public MessageProducer(KafkaOptions options) {
        this.options = options;
    }

    @Override
    public PipelineResult.State runPipeline() {

        //每30秒钟，发送10条数据
        GenerateSequence sequence = GenerateSequence.from(0)
                .withRate(KafkaOptions.MESSAGES_COUNT, Duration.standardSeconds(KafkaOptions.WINDOW_TIME));
        sequence.withTimestampFn((Long n) -> new Instant(System.currentTimeMillis()));

        Pipeline pipeline = Pipeline.create(options);
        //读取一条数据
        PCollection<SchoolClass> inputSchoolClass = pipeline.apply(sequence).apply("#ProducerSchoolClassMsg",
                ParDo.of(new GeneratorSchoolClassDoFn()));
        //转换为JSON格式
        PCollection<KV<Long, String>> JsonDataPc = inputSchoolClass.apply(ParDo.of(new ObjectMapperJson.WithKeyDataStingConvertDoFn<>()));
        KafkaIO.Write<Long, String> kafkaIo = KafkaIO.<Long, String>write()
                .withBootstrapServers(options.getKafkaHost())
                .withTopic(options.getTopic())
                .withKeySerializer(LongSerializer.class)
                .withValueSerializer(StringSerializer.class)
                .withProducerConfigUpdates(ImmutableMap.of("group.id", "beam_process_1"));
        //通过Kafka发送消息
        JsonDataPc.apply(kafkaIo);
        return pipeline.run().waitUntilFinish();
    }
}
