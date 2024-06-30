package com.beam.project.demo.window.common;

import com.beam.project.common.KafkaOptions;
import com.beam.project.common.LogOutput;
import com.google.common.collect.ImmutableMap;
import org.apache.beam.sdk.io.kafka.KafkaIO;
import org.apache.beam.sdk.transforms.PTransform;
import org.apache.beam.sdk.transforms.ParDo;
import org.apache.beam.sdk.transforms.Values;
import org.apache.beam.sdk.values.PBegin;
import org.apache.beam.sdk.values.PCollection;
import org.apache.kafka.common.serialization.LongDeserializer;
import org.apache.kafka.common.serialization.StringDeserializer;

@SuppressWarnings("all")
public class KafkaMessageReadPF extends PTransform<PBegin, PCollection<String>> {

    public KafkaOptions options = null;

    public KafkaMessageReadPF(KafkaOptions options) {
        this.options = options;
    }

    @Override
    public PCollection<String> expand(PBegin input) {

        PCollection<String> kafkaMessage = input.apply("#ConsumerSchoolClassMsg",
                        KafkaIO.<Long, String>read()
                                .withBootstrapServers(options.getKafkaHost())
                                .withTopic(options.getTopic())
                                .withKeyDeserializer(LongDeserializer.class)
                                .withValueDeserializer(StringDeserializer.class)
                                .withConsumerConfigUpdates(ImmutableMap.of("group.id", "beam_process_1"))
                                .withReadCommitted()
                                .withoutMetadata())
                .apply(Values.create());
        return kafkaMessage;
    }
}
