package com.beam.project.common;

import org.apache.beam.sdk.options.Default;
import org.apache.beam.sdk.options.Description;
import org.apache.beam.sdk.options.PipelineOptions;

public interface KafkaOptions extends PipelineOptions, LocalFileOptions {

    @Description("Kafka server host")
    @Default.String("data-master01:9092")
    String getKafkaHost();

    void setKafkaHost(String value);

}
