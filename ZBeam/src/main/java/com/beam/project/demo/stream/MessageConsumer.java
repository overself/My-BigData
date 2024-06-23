package com.beam.project.demo.stream;

import com.beam.project.common.KafkaOptions;
import com.beam.project.common.LogOutput;
import com.beam.project.common.PipelineRunner;
import com.beam.project.core.convert.ObjectMapperJson;
import com.beam.project.demo.bean.ExamScore;
import com.beam.project.demo.bean.SchoolClass;
import com.beam.project.demo.stream.transform.CalcClassSubjectAverageTransform;
import com.beam.project.demo.stream.transform.CalcSchoolSubjectAverageTransform;
import com.beam.project.demo.stream.transform.ReadSubjectScoreDoFn;
import com.beam.project.demo.stream.transform.SchoolClassDistinctTransform;
import com.google.common.collect.ImmutableMap;
import org.apache.beam.sdk.Pipeline;
import org.apache.beam.sdk.PipelineResult;
import org.apache.beam.sdk.coders.SerializableCoder;
import org.apache.beam.sdk.io.kafka.KafkaIO;
import org.apache.beam.sdk.transforms.ParDo;
import org.apache.beam.sdk.transforms.windowing.*;
import org.apache.beam.sdk.values.KV;
import org.apache.beam.sdk.values.PCollection;
import org.apache.kafka.common.serialization.LongDeserializer;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.joda.time.Duration;

public class MessageConsumer implements PipelineRunner {

    private KafkaOptions options;

    public MessageConsumer(KafkaOptions options) {
        this.options = options;
    }

    @Override
    public PipelineResult.State runPipeline() {
        options.setParallelism(1);
        Pipeline pipeline = Pipeline.create(options);

        PCollection<KV<Long, String>> kafkaMessage = pipeline.apply("#ConsumerSchoolClassMsg",
                KafkaIO.<Long, String>read()
                        .withBootstrapServers(options.getKafkaHost())
                        .withTopic(options.getTopic())
                        .withKeyDeserializer(LongDeserializer.class)
                        .withValueDeserializer(StringDeserializer.class)
                        .withConsumerConfigUpdates(ImmutableMap.of("group.id", "beam_process_1"))
                        .withReadCommitted().withoutMetadata());
        kafkaMessage.apply("#Log001", ParDo.of(new LogOutput<>("原消息")));

        // Create fixed window for the length of the game round
        // 处理触发条件
//OK        Trigger trigger = AfterWatermark.pastEndOfWindow(); //OK
        Trigger trigger = DefaultTrigger.of();
//NG        Trigger afterProcessing = AfterProcessingTime.pastFirstElementInPane().plusDelayOf(Duration.standardSeconds(KafkaOptions.WINDOW_TIME * 2));
//NG        Trigger afterWatermark = AfterWatermark.pastEndOfWindow().withEarlyFirings(AfterProcessingTime.pastFirstElementInPane().plusDelayOf(Duration.standardSeconds(KafkaOptions.WINDOW_TIME * 2)))
//                .withLateFirings(AfterPane.elementCountAtLeast(1));

        Window<KV<Long, String>> window = Window.into(FixedWindows.of(Duration.standardMinutes(1)));
        PCollection<KV<Long, String>> windowedRecords = kafkaMessage.apply("#ReceiveWindowedSchoolClass", window
                .triggering(trigger)
                //.withAllowedLateness(Duration.standardSeconds(1))
                .withAllowedLateness(Duration.ZERO)
                .discardingFiredPanes());
        //windowedRecords.apply(ParDo.of(new LogOutput<>("窗口消息")));

        //PCollection<SchoolClass> scorePc = windowedRecords.apply(MapElements.via(new ObjectMapperJson.ConvertToRecodeFunction<>(SchoolClass.class))).setCoder(SerializableCoder.of(SchoolClass.class));
        PCollection<SchoolClass> schoolClassRecords = windowedRecords.apply(ParDo.of(new ObjectMapperJson.ConvertToRecodeDoFn<>(SchoolClass.class))).setCoder(SerializableCoder.of(SchoolClass.class));
        schoolClassRecords.apply(ParDo.of(new LogOutput<>("转班级")));

        //对每个时间窗口的数据进行GroupByKey分组去重，去重key：SCHOOL && CLASS
        PCollection<SchoolClass> distinctSchoolClasses = schoolClassRecords.apply(new SchoolClassDistinctTransform());
        distinctSchoolClasses.apply("#Log002", ParDo.of(new LogOutput<>("去重后")));

        //获取指定班级全部学生成绩(2科*10人)，条件：SCHOOL && CLASS -> SCHOOL、CLASS、SUBJECT、STUDENT、SCORE
        PCollection<ExamScore> classSubjectScores = distinctSchoolClasses.apply(ParDo.of(new ReadSubjectScoreDoFn()));
        //classSubjectScores.apply(ParDo.of(new LogOutput<>("获取班级各科")));

        //按照学科统计班级的各科成绩平均分，分组Key：SCHOOL && CLASS && SUBJECT ->SCHOOL、CLASS、SUBJECT、AVG(SCORE)
        PCollection<ExamScore> classAveragePC = classSubjectScores.apply(new CalcClassSubjectAverageTransform());
        classAveragePC.apply("#Log003", ParDo.of(new LogOutput<>("计算班级各科")));

        //按照学科统计学校的成绩平均分，分组Key：SCHOOL && SUBJECT ->SCHOOL、SUBJECT、AVG(SCORE)
        PCollection<ExamScore> schoolAveragePC = classAveragePC.apply(new CalcSchoolSubjectAverageTransform());
        schoolAveragePC.apply("#Log004", ParDo.of(new LogOutput<>("计算学校各科")));

        return pipeline.run().waitUntilFinish();
    }


}
