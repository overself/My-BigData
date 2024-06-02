package com.beam.project.demo.stream;

import avro.shaded.com.google.common.collect.ImmutableMap;
import com.beam.project.common.KafkaOptions;
import com.beam.project.common.LogOutput;
import com.beam.project.common.PipelineRunner;
import com.beam.project.core.convert.ObjectMapperJson;
import com.beam.project.demo.bean.ExamScore;
import com.beam.project.demo.bean.SchoolClass;
import com.beam.project.demo.stream.transform.CalcClassSubjectAverageTransform;
import com.beam.project.demo.stream.transform.CalcSchoolSubjectAverageTransform;
import com.beam.project.demo.stream.transform.SchoolClassDistinctTransform;
import org.apache.beam.sdk.Pipeline;
import org.apache.beam.sdk.PipelineResult;
import org.apache.beam.sdk.coders.SerializableCoder;
import org.apache.beam.sdk.io.kafka.KafkaIO;
import org.apache.beam.sdk.transforms.*;
import org.apache.beam.sdk.transforms.windowing.*;
import org.apache.beam.sdk.values.KV;
import org.apache.beam.sdk.values.PCollection;
import org.apache.kafka.common.serialization.LongDeserializer;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.joda.time.Duration;

import java.util.List;

public class MessageConsumer implements PipelineRunner {

    private KafkaOptions options;

    public MessageConsumer(KafkaOptions options) {
        this.options = options;
    }

    @Override
    public PipelineResult.State runPipeline() {
        Pipeline pipeline = Pipeline.create(options);

        PCollection<KV<Long, String>> kafkaMessage = pipeline.apply("#ConsumerSchoolClassMsg",
                KafkaIO.<Long, String>read()
                        .withBootstrapServers(options.getKafkaHost())
                        .withTopic(options.getTopic())
                        .withKeyDeserializer(LongDeserializer.class)
                        .withValueDeserializer(StringDeserializer.class)
                        .withConsumerConfigUpdates(ImmutableMap.of("group.id", "beam_process_1"))
                        .withReadCommitted().withoutMetadata());
        //kafkaMessage.apply(ParDo.of(new LogOutput<>("原始消息")));

        // Create fixed window for the length of the game round
        // 处理触发条件：每4分钟，或者每3条数据
        Window<KV<Long, String>> window = Window.into(FixedWindows.of(Duration.standardSeconds(KafkaOptions.WINDOW_TIME * 2)));
        Trigger trigger = AfterProcessingTime.pastFirstElementInPane().plusDelayOf(Duration.standardSeconds(KafkaOptions.WINDOW_TIME * 2));
        PCollection<KV<Long, String>> windowedRecords = kafkaMessage.apply("#ReceiveWindowedSchoolClass", window
                .triggering(Repeatedly.forever(trigger))
                .withAllowedLateness(Duration.standardSeconds(1))
                .discardingFiredPanes());
        //windowedRecords.apply(ParDo.of(new LogOutput<>("窗口消息")));

        //PCollection<SchoolClass> scorePc = windowedRecords.apply(MapElements.via(new ObjectMapperJson.ConvertToRecodeFunction<>(SchoolClass.class))).setCoder(SerializableCoder.of(SchoolClass.class));
        PCollection<SchoolClass> schoolClassRecords = windowedRecords.apply(ParDo.of(new ObjectMapperJson.ConvertToRecodeDoFn<>(SchoolClass.class))).setCoder(SerializableCoder.of(SchoolClass.class));
        schoolClassRecords.apply(ParDo.of(new LogOutput<>("转换至班级")));

        //对每个时间窗口的数据进行GroupByKey分组去重，去重key：SCHOOL && CLASS
        PCollection<SchoolClass> distinctSchoolClasses = schoolClassRecords.apply(new SchoolClassDistinctTransform());
        distinctSchoolClasses.apply(ParDo.of(new LogOutput<>("去重结果")));

        //获取指定班级全部学生成绩，条件：SCHOOL && CLASS -> SCHOOL、CLASS、SUBJECT、STUDENT、SCORE
        PCollection<ExamScore> classSubjectScores = distinctSchoolClasses.apply(ParDo.of(new ReadSubjectScoreDoFn()));
        classSubjectScores.apply(ParDo.of(new LogOutput<>("获取班级各科成绩")));

        //按照学科统计班级的各科成绩平均分，分组Key：SCHOOL && CLASS && SUBJECT ->SCHOOL、CLASS、SUBJECT、AVG(SCORE)
        PCollection<ExamScore> classAveragePC = classSubjectScores.apply(new CalcClassSubjectAverageTransform());
        classAveragePC.apply(ParDo.of(new LogOutput<>("计算班级各科平均分")));

        //按照学科统计学校的成绩平均分，分组Key：SCHOOL && SUBJECT ->SCHOOL、SUBJECT、AVG(SCORE)
        PCollection<ExamScore> schoolAveragePC = classSubjectScores.apply(new CalcSchoolSubjectAverageTransform());
        schoolAveragePC.apply(ParDo.of(new LogOutput<>("计算学校各科平均分")));

        return pipeline.run().waitUntilFinish();
    }


    private static class ReadSubjectScoreDoFn extends DoFn<SchoolClass, ExamScore> {
        @ProcessElement
        public void processElement(ProcessContext c) {
            SchoolClass schoolClass = c.element();
            List<ExamScore> scores = GeneratorScore.getSubjectScore(schoolClass);
            scores.forEach(score -> c.output(score));
        }
    }
}
