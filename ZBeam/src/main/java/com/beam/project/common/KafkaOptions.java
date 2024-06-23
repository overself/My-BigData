package com.beam.project.common;

import org.apache.beam.runners.flink.FlinkPipelineOptions;
import org.apache.beam.sdk.options.Default;
import org.apache.beam.sdk.options.Description;

public interface KafkaOptions extends FlinkPipelineOptions {

    /**
     * Playground's Kafka server
     */
    @Description("Kafka server host")
    @Default.String("data-master01:9092,data-worker01:9092,data-worker02:9092")
    String getKafkaHost();

    void setKafkaHost(String value);

    @Description("Kafka Message Topic")
    @Default.String("BeamTest")
    String getTopic();

    void setTopic(String topic);

    int WINDOW_TIME = 30;

    int MESSAGES_COUNT = 10;
}

