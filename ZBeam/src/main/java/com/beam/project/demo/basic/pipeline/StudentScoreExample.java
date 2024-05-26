package com.beam.project.demo.basic.pipeline;

import com.beam.project.common.LogOutput;
import com.google.api.client.util.Lists;
import lombok.Data;
import org.apache.beam.runners.direct.DirectRunner;
import org.apache.beam.runners.flink.FlinkRunner;
import org.apache.beam.sdk.Pipeline;
import org.apache.beam.sdk.coders.SerializableCoder;
import org.apache.beam.sdk.options.PipelineOptions;
import org.apache.beam.sdk.options.PipelineOptionsFactory;
import org.apache.beam.sdk.transforms.*;
import org.apache.beam.sdk.transforms.windowing.FixedWindows;
import org.apache.beam.sdk.transforms.windowing.Window;
import org.apache.beam.sdk.values.KV;
import org.apache.beam.sdk.values.PCollection;
import org.apache.commons.lang3.RandomUtils;
import org.checkerframework.checker.initialization.qual.Initialized;
import org.checkerframework.checker.nullness.qual.NonNull;
import org.checkerframework.checker.nullness.qual.UnknownKeyFor;
import org.joda.time.Duration;

import java.io.Serializable;
import java.util.Arrays;
import java.util.List;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.atomic.AtomicLong;

public class StudentScoreExample {

    static AtomicLong count = new AtomicLong(0);

    private static final String[] SCHOOL_CODE = {"SC01", "SC02", "SC03"};

    private static final String[] CLASS_CODE = {"CL01", "CL02", "CL03", "CL04", "CL05", "CL06"};

    private static final String[] SUBJECT_CODE = {"maths", "English", "physics", "Chinese"};

    private static final String[] STUDENT_CODE =
            {"ST01", "ST02", "ST03", "ST04", "ST05", "ST06", "ST07", "ST08", "ST09", "ST10",
                    "ST11", "ST12", "ST13", "ST14", "ST15", "ST16", "ST17", "ST18", "ST19", "ST20",
                    "ST21", "ST22", "ST23", "ST24", "ST25", "ST26", "ST27", "ST28", "ST29", "ST30",
                    "ST31", "ST32", "ST33", "S34", "ST35", "ST36", "ST37", "ST38", "ST39", "ST40"};

    public static void main(String[] args) {
        PipelineOptions options = PipelineOptionsFactory.create();
        options.setRunner(DirectRunner.class);
        Pipeline pipeline = Pipeline.create(options);

        List<StudentScore> scores = getStudentScores();
        List<Long> timestamps = Lists.newArrayList();
        for (int stimes = 0; stimes < scores.size(); stimes++) {
            timestamps.add(Duration.standardSeconds(stimes).getMillis());
        }

        // Create a PCollection from the input data with timestamps
        PCollection<StudentScore> items = pipeline.apply(Create.timestamped(scores, timestamps));

        // Create a windowed PCollection
        PCollection<StudentScore> windowedItems = items.apply(
                Window.into(FixedWindows.of(Duration.standardMinutes(1))));

        //每个窗口收集到的数据个数
        windowedItems.apply(ParDo.of(new LogOutput<>("Windowed Items")));
        PCollection<Long> count = windowedItems.apply("CountElements",
                Combine.globally(Count.<StudentScore>combineFn()).withoutDefaults());
        count.apply(ParDo.of(new LogOutput<>("Windowed Elements Count")));
        //对每个时间窗口的数据进行分组
        PCollection<KV<String, Iterable<StudentScore>>> windowedCountsGroup = windowedItems.apply(
                WithKeys.of(new SimpleFunction<StudentScore, String>() {
                    @Override
                    public String apply(StudentScore input) {
                        return input.schoolCode + "," + input.classCode;
                    }
                })).apply(GroupByKey.create());
        windowedCountsGroup.apply(ParDo.of(new LogOutput<>("StudentCountsGroup")));

        //去重处理
        PCollection<StudentScore> distinctScores = windowedItems
                .apply(Distinct.withRepresentativeValueFn(new SimpleFunction<StudentScore, String>() {
                    @Override
                    public String apply(StudentScore input) {
                        return input.schoolCode + "," + input.classCode + "," + input.studentCode + "," + input.subject;
                    }
                }));
        PCollection<Long> distinctCount = distinctScores.apply("DistinctCount",
                Combine.globally(Count.<StudentScore>combineFn()).withoutDefaults());
        distinctCount.apply(ParDo.of(new LogOutput<>("Distinct Elements Count")));
        //对每个时间窗口的数据进行分组
        PCollection<KV<String, Iterable<StudentScore>>> windowedDistinctGroup = distinctScores.apply(
                WithKeys.of(new SimpleFunction<StudentScore, String>() {
                    @Override
                    public String apply(StudentScore input) {
                        return input.schoolCode + "," + input.classCode;
                    }
                })).apply(GroupByKey.create());
        windowedDistinctGroup.apply(ParDo.of(new LogOutput<>("DistinctScoresGroup")));

        pipeline.run();
    }

    private static List<StudentScore> getStudentScores() {
        List<StudentScore> scores = Lists.newArrayList();
        for (int index = 0; index < 360; index++) {
            StudentScore score = new StudentScore();
            score.setSchoolCode(SCHOOL_CODE[RandomUtils.nextInt(0, 2)]);
            score.setClassCode(CLASS_CODE[RandomUtils.nextInt(0, 5)]);
            score.setSubject(SUBJECT_CODE[RandomUtils.nextInt(0, 3)]);
            score.setStudentCode(STUDENT_CODE[RandomUtils.nextInt(0, 39)]);
            score.setScore(RandomUtils.nextInt(65, 100));
            scores.add(score);
        }
        return scores;
    }

    @Data
    public static class StudentScore implements Serializable {

        private String studentCode;

        private String schoolCode;

        private String classCode;

        private String subject;

        private Integer score;
    }
}
