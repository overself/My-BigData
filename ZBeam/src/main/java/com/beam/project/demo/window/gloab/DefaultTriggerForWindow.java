package com.beam.project.demo.window.gloab;

import com.beam.project.common.KafkaOptions;
import com.beam.project.common.LogOutput;
import com.beam.project.core.convert.ObjectMapperJson;
import com.beam.project.demo.bean.SchoolClass;
import com.beam.project.demo.window.common.KafkaMessageReadPF;
import lombok.extern.slf4j.Slf4j;
import org.apache.beam.runners.flink.FlinkRunner;
import org.apache.beam.sdk.Pipeline;
import org.apache.beam.sdk.PipelineResult;
import org.apache.beam.sdk.coders.SerializableCoder;
import org.apache.beam.sdk.options.PipelineOptionsFactory;
import org.apache.beam.sdk.transforms.Distinct;
import org.apache.beam.sdk.transforms.ParDo;
import org.apache.beam.sdk.transforms.SimpleFunction;
import org.apache.beam.sdk.transforms.windowing.*;
import org.apache.beam.sdk.values.PCollection;
import org.joda.time.Duration;

@Slf4j
@SuppressWarnings("all")
public class DefaultTriggerForWindow {

    public static void main(String[] args) {

        KafkaOptions options = PipelineOptionsFactory.fromArgs(args).withValidation().as(KafkaOptions.class);
        options.setTopic("SchoolClassTopic");
        options.setRunner(FlinkRunner.class);
        options.setParallelism(6);

        //
        run001(options);


    }

    private static void run001(KafkaOptions options) {
        log.info("Run001 Pipeline Start");
        Pipeline pipeline = Pipeline.create(options);
        PCollection<SchoolClass> schoolClassRecords = windowBefore(pipeline, options);

        Window<SchoolClass> window = Window.into(new GlobalWindows());
        PCollection<SchoolClass> windowedRecords = schoolClassRecords.apply("#ReceiveWindowedData", window
                .triggering(Repeatedly.forever(AfterProcessingTime.pastFirstElementInPane().plusDelayOf(Duration.standardSeconds(10))))
                .withAllowedLateness(Duration.ZERO)
                .discardingFiredPanes());

        schoolClassRecords = windowAfter(windowedRecords);
        PipelineResult.State state = pipeline.run().waitUntilFinish();
        log.info("Run001 Pipeline End");
    }


    /**
     * @param options
     */
    private static void run002(KafkaOptions options) {
        log.info("Run001 Pipeline Start");
        Pipeline pipeline = Pipeline.create(options);
        PCollection<SchoolClass> schoolClassRecords = windowBefore(pipeline, options);

        Window<SchoolClass> window = Window.into(new GlobalWindows());
        PCollection<SchoolClass> windowedRecords = schoolClassRecords.apply("#ReceiveWindowedData", window
                .triggering(Repeatedly.forever(AfterPane.elementCountAtLeast(5)))
                .discardingFiredPanes());

        schoolClassRecords = windowAfter(windowedRecords);
        PipelineResult.State state = pipeline.run().waitUntilFinish();
        log.info("Run001 Pipeline End");
    }

    private static PCollection<SchoolClass> windowBefore(Pipeline pipeline, KafkaOptions options) {
        PCollection<String> kafkaMessage = pipeline.apply("#ConsumerMsg", new KafkaMessageReadPF(options));
        kafkaMessage.apply("#Log001", ParDo.of(new LogOutput<>("原消息")));

        PCollection<SchoolClass> schoolClassRecords = kafkaMessage.apply("#ConvertMsg",
                ParDo.of(new ObjectMapperJson.StrToRecodeFunction<>(SchoolClass.class))).setCoder(SerializableCoder.of(SchoolClass.class));
        schoolClassRecords.apply(ParDo.of(new LogOutput<>("转班级")));

        return schoolClassRecords;
    }

    private static PCollection<SchoolClass> windowAfter(PCollection<SchoolClass> windowedRecords) {
        PCollection<SchoolClass> windowedSchool = windowedRecords.apply("#Distinct", Distinct.withRepresentativeValueFn(new SimpleFunction<SchoolClass, String>() {
            @Override
            public String apply(SchoolClass input) {
                return input.getSchoolClassKey();
            }
        }));
        windowedSchool.apply("#WindowedData", ParDo.of(new LogOutput<>("班级信息")));
        return windowedSchool;
    }
}
