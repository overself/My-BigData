package com.beam.project.common;

import org.apache.beam.sdk.options.Default;
import org.apache.beam.sdk.options.Description;
import org.apache.beam.sdk.options.PipelineOptions;

public interface KafkaOptions extends PipelineOptions {

    /**
     * Playground's Kafka server
     */
    @Description("Kafka server host")
    @Default.String("data-master01:9092,data-data-worker01:9092,data-worker02:9092")
    String getKafkaHost();

    void setKafkaHost(String value);

    @Description("Kafka server host")
    @Default.String("BeamTest")
    String getTopic();

    void setTopic(String topic);
}

