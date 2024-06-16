package com.beam.project.demo.window;

import com.beam.project.common.SnowFlakeUtil;
import com.beam.project.core.convert.CustomObjectMapper;
import com.beam.project.core.convert.ObjectMapperJson;
import com.beam.project.demo.bean.SchoolClass;
import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.common.collect.ImmutableMap;
import lombok.Setter;
import lombok.SneakyThrows;
import lombok.extern.slf4j.Slf4j;
import org.apache.beam.sdk.Pipeline;
import org.apache.beam.sdk.io.GenerateSequence;
import org.apache.beam.sdk.io.kafka.KafkaIO;
import org.apache.beam.sdk.options.PipelineOptions;
import org.apache.beam.sdk.options.PipelineOptionsFactory;
import org.apache.beam.sdk.transforms.DoFn;
import org.apache.beam.sdk.transforms.ParDo;
import org.apache.beam.sdk.values.KV;
import org.apache.beam.sdk.values.PCollection;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.Producer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.serialization.LongSerializer;
import org.apache.kafka.common.serialization.StringSerializer;
import org.joda.time.Duration;
import org.joda.time.Instant;

import java.util.Properties;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicLong;

@Slf4j
public class SchoolMessageProducer {

    private final CustomObjectMapper objectMapper = new CustomObjectMapper();

    private final Properties props = new Properties();

    @Setter
    private long messageCount = 100L;

    @Setter
    private boolean isTaskSend = false;

    public SchoolMessageProducer(String kafkaServers, String topic) {
        props.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, kafkaServers);
        props.put("Message_Topic", topic);
        props.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, LongSerializer.class);
        props.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class);
    }

    @SneakyThrows
    public void doSendMessage() {
        if (this.isTaskSend) {
            beamPipeline();
        } else {
            fixedMessageSend();
        }
    }

    private void fixedMessageSend() throws JsonProcessingException {
        // 其他配置...
        Producer<Long, String> producer = new KafkaProducer<>(this.props);
        long startTime = System.nanoTime();
        AtomicLong atomicLong = new AtomicLong(0);
        for (long i = 0; i < 100; i++) {
            Long dataKey = SnowFlakeUtil.getSnowFlakeId();
            SchoolClass schoolClass = new SchoolClass();
            schoolClass.setSchoolCode("S" + (i % 4));
            schoolClass.setClassCode("C" + dataKey);
            schoolClass.setTimestamp(Instant.now());
            //schoolClass.setSchoolCode("S" + dataKey);
            //schoolClass.setClassCode("C" + (i % 4));
            producer.send(new ProducerRecord<Long, String>("SchoolClassTopic", dataKey,
                            objectMapper.writeValueAsString(schoolClass)),
                    (metadata, exception) -> {
                        if (exception != null) {
                            log.error("SchoolClass Send Fail: {}", schoolClass);
                        }
                    });
            atomicLong.getAndIncrement();
        }
        producer.flush(); // 确保所有消息都发送完毕
        long endTime = System.nanoTime();
        log.info("Messages sent in {} ms， Count：{}", TimeUnit.NANOSECONDS.toMillis(endTime - startTime), atomicLong.get());
        producer.close();
    }

    private void beamPipeline() {
        PipelineOptions options = PipelineOptionsFactory.create();
        Pipeline pipeline = Pipeline.create(options);
        //每分钟发送指定个数的数据
        GenerateSequence sequence = GenerateSequence.from(0).withRate(messageCount, Duration.standardMinutes(1));
        sequence.withTimestampFn((Long n) -> new Instant(System.currentTimeMillis()));
        sequence.withMaxReadTime(Duration.standardMinutes(2));

        PCollection<SchoolClass> input = pipeline.apply(sequence).apply(ParDo.of(new RandomGeneratorFn(new AtomicLong(0))));
        PCollection<KV<Long, String>> JsonDataPc = input.apply(ParDo.of(new ObjectMapperJson.WithKeyDataStingConvertDoFn<SchoolClass>()));
        KafkaIO.Write<Long, String> kafkaIo = KafkaIO.<Long, String>write()
                .withBootstrapServers(props.getProperty(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG))
                .withTopic(props.getProperty("Message_Topic"))
                .withKeySerializer(LongSerializer.class)
                .withValueSerializer(StringSerializer.class)
                .withProducerConfigUpdates(ImmutableMap.of("group.id", "beam_score_1"));
        JsonDataPc.apply(kafkaIo);
        pipeline.run().waitUntilFinish();
    }

    public static class RandomGeneratorFn extends DoFn<Long, SchoolClass> {

        private AtomicLong atomicIncrement;

        public RandomGeneratorFn(AtomicLong atomicLong) {
            this.atomicIncrement = atomicLong;
        }

        @ProcessElement
        public void processElement(ProcessContext c) {
            Long dataKey = SnowFlakeUtil.getSnowFlakeId();
            SchoolClass schoolClass = new SchoolClass();
            schoolClass.setSchoolCode("S" + (atomicIncrement.getAndIncrement() % 4));
            schoolClass.setClassCode("C" + dataKey);
            schoolClass.setTimestamp(new Instant(System.currentTimeMillis()));
            c.output(schoolClass);
        }
    }

}
