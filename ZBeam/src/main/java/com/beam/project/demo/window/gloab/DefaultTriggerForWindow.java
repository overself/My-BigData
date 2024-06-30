package com.beam.project.demo.window.gloab;

import com.beam.project.common.KafkaOptions;
import com.beam.project.common.LogOutput;
import com.beam.project.core.convert.ObjectMapperJson;
import com.beam.project.demo.bean.SchoolClass;
import com.beam.project.demo.window.common.KafkaMessageReadPF;
import com.google.common.collect.ImmutableMap;
import lombok.extern.slf4j.Slf4j;
import org.apache.beam.runners.flink.FlinkRunner;
import org.apache.beam.sdk.Pipeline;
import org.apache.beam.sdk.PipelineResult;
import org.apache.beam.sdk.coders.SerializableCoder;
import org.apache.beam.sdk.io.kafka.KafkaIO;
import org.apache.beam.sdk.options.PipelineOptionsFactory;
import org.apache.beam.sdk.transforms.Distinct;
import org.apache.beam.sdk.transforms.ParDo;
import org.apache.beam.sdk.transforms.SimpleFunction;
import org.apache.beam.sdk.transforms.Values;
import org.apache.beam.sdk.transforms.windowing.*;
import org.apache.beam.sdk.values.KV;
import org.apache.beam.sdk.values.PCollection;
import org.apache.kafka.common.serialization.LongDeserializer;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.joda.time.Duration;

@Slf4j
@SuppressWarnings("all")
public class DefaultTriggerForWindow {

    public static void main(String[] args) {

        KafkaOptions options = PipelineOptionsFactory.fromArgs(args).withValidation().as(KafkaOptions.class);
        options.setTopic("SchoolClassTopic");
        options.setRunner(FlinkRunner.class);
        options.setParallelism(6);

        Pipeline pipeline = Pipeline.create(options);

        PCollection<String> kafkaMessage = pipeline.apply("#ConsumerMsg", new KafkaMessageReadPF(options));
        kafkaMessage.apply("#Log001", ParDo.of(new LogOutput<>("原消息")));

        PCollection<SchoolClass> schoolClassRecords = kafkaMessage.apply("#ConvertMsg",
                ParDo.of(new ObjectMapperJson.StrToRecodeFunction<>(SchoolClass.class))).setCoder(SerializableCoder.of(SchoolClass.class));
        schoolClassRecords.apply(ParDo.of(new LogOutput<>("转班级")));

        Window<SchoolClass> window = Window.into(new GlobalWindows());
        PCollection<SchoolClass> windowedRecords = schoolClassRecords.apply("#ReceiveWindowedData", window
                .triggering(Repeatedly.forever(AfterPane.elementCountAtLeast(5)))
                .discardingFiredPanes());

        PCollection<SchoolClass> windowedSchool = windowedRecords.apply("#Distinct", Distinct.withRepresentativeValueFn(new SimpleFunction<SchoolClass, String>() {
            @Override
            public String apply(SchoolClass input) {
                return input.getSchoolClassKey();
            }
        }));
        windowedSchool.apply("#WindowedData", ParDo.of(new LogOutput<>("班级信息")));

        PipelineResult.State state = pipeline.run().waitUntilFinish();
        log.info("PipelineResult.State: {}", state);
    }


}
