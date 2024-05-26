package com.beam.project.demo.basic.pipeline;

import com.beam.project.common.LogOutput;
import org.apache.beam.runners.flink.FlinkRunner;
import org.apache.beam.sdk.Pipeline;
import org.apache.beam.sdk.options.PipelineOptions;
import org.apache.beam.sdk.options.PipelineOptionsFactory;
import org.apache.beam.sdk.transforms.*;
import org.apache.beam.sdk.transforms.windowing.FixedWindows;
import org.apache.beam.sdk.transforms.windowing.Window;
import org.apache.beam.sdk.values.KV;
import org.apache.beam.sdk.values.PCollection;
import org.checkerframework.checker.initialization.qual.Initialized;
import org.checkerframework.checker.nullness.qual.Nullable;
import org.checkerframework.checker.nullness.qual.UnknownKeyFor;
import org.joda.time.Duration;

import java.util.Arrays;
import java.util.List;

public class WindowExample {

    public static void main(String[] args) {
        PipelineOptions options = PipelineOptionsFactory.create();
        options.setRunner(FlinkRunner.class);
        Pipeline pipeline = Pipeline.create(options);

        // [START main_section]
        // Create some input data with timestamps
        List<String> inputData = Arrays.asList("foo", "bar", "bar", "foo");
        List<Long> timestamps = Arrays.asList(Duration.standardSeconds(30).getMillis(),
                Duration.standardSeconds(60).getMillis(),
                Duration.standardSeconds(120).getMillis(),
                Duration.standardSeconds(180).getMillis());

        // Create a PCollection from the input data with timestamps
        PCollection<String> items = pipeline.apply(Create.timestamped(inputData, timestamps));

        // Create a windowed PCollection
        PCollection<String> windowedItems =
                items.apply(Window.into(FixedWindows.of(Duration.standardMinutes(1))));

        PCollection<KV<String, Long>> windowedCounts = windowedItems.apply(Count.perElement());
        // [END main_section]
        windowedCounts.apply(ParDo.of(new LogOutput<>("PCollection elements after Count transform")));

        //PCollection<Long> count = windowedItems.apply("CountElements", Combine.globally(Count.combineFn()));
        PCollection<Long> count = windowedItems.apply("CountElements", Combine.globally(Count.<String>combineFn()).withoutDefaults());

        count.apply(ParDo.of(new LogOutput<>("PCollection count")));

        pipeline.run();
    }
}
