package com.beam.project.demo.basic.pipeline;

import com.beam.project.common.LogOutput;
import com.beam.project.common.SnowFlakeUtil;
import com.google.api.client.util.Lists;
import lombok.Data;
import org.apache.beam.runners.flink.FlinkRunner;
import org.apache.beam.sdk.Pipeline;
import org.apache.beam.sdk.options.PipelineOptions;
import org.apache.beam.sdk.options.PipelineOptionsFactory;
import org.apache.beam.sdk.transforms.*;
import org.apache.beam.sdk.transforms.windowing.FixedWindows;
import org.apache.beam.sdk.transforms.windowing.Window;
import org.apache.beam.sdk.values.KV;
import org.apache.beam.sdk.values.PCollection;
import org.apache.commons.lang3.RandomUtils;
import org.joda.time.Duration;

import java.io.Serializable;
import java.util.List;

public class StudentScoreExample {
    private static final String[] SCHOOL_CODE = {"SC01", "SC02", "SC03"};

    private static final String[] CLASS_CODE = {"CL01", "CL02", "CL03", "CL04", "CL05", "CL06"};

    private static final String[] SUBJECT_CODE = {"maths", "English", "physics", "Chinese"};

    private static final String[] STUDENT_CODE =
            {"ST01", "ST02", "ST03", "ST04", "ST05", "ST06", "ST07", "ST08", "ST09", "ST10",
                    "ST11", "ST12", "ST13", "ST14", "ST15", "ST16", "ST17", "ST18", "ST19", "ST20",
                    "ST21", "ST22", "ST23", "ST24", "ST25", "ST26", "ST27", "ST28", "ST29", "ST30",
                    "ST31", "ST32", "ST33", "S34", "ST35", "ST36", "ST37", "ST38", "ST39", "ST40"};

    private static final String[] CLASS_CODE_FIXED = {"CL01", "CL02", "CL03"};

    private static final String[] STUDENT_CODE_FIXED =
            {"ST01", "ST02", "ST03", "ST04", "ST05", "ST06", "ST07", "ST08", "ST09", "ST10"};

    public static void main(String[] args) {
        PipelineOptions options = PipelineOptionsFactory.create();
        options.setRunner(FlinkRunner.class);
        Pipeline pipeline = Pipeline.create(options);

        //List<StudentScore> scores = getRandomStudentScores();
        List<StudentScore> scores = getFixedStudentScores();

        List<Long> timestamps = Lists.newArrayList();
        for (int stimes = 0; stimes < scores.size(); stimes++) {
            timestamps.add(Duration.standardSeconds(stimes).getMillis());
        }

        // Create a PCollection from the input data with timestamps
        PCollection<StudentScore> items = pipeline.apply(Create.timestamped(scores, timestamps));

        // Create a windowed PCollection
        PCollection<StudentScore> windowedItems = items.apply(Window.into(FixedWindows.of(Duration.standardMinutes(3))));

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

        //排除物理科目的统计,并给出分校区名称
        PCollection<StudentScore> mainSubjectScores = distinctScores.apply(ParDo.of(new DoFn<StudentScore, StudentScore>() {
            @ProcessElement
            public void process(@Element StudentScore score, OutputReceiver<StudentScore> receiver) throws CloneNotSupportedException {
                if ("physics".equals(score.getSubject())) {
                    return;
                }
                StudentScore scoreNew = score.clone();
                scoreNew.setSchoolName("实验中学" + score.getSchoolCode() + "分校区");
                receiver.output(scoreNew);
            }
        }));

        //计算每个班级的单科平均分
        PCollection<StudentScore> schoolClassSubjectAverage = mainSubjectScores.apply(new PTransform<PCollection<StudentScore>, PCollection<StudentScore>>() {
            @Override
            public PCollection<StudentScore> expand(PCollection<StudentScore> input) {
                return input.apply(WithKeys.of(new SimpleFunction<StudentScore, String>() {
                            @Override
                            public String apply(StudentScore input) {
                                return input.schoolCode + "," + input.classCode + "," + input.subject;
                            }
                        }))
                        .apply(GroupByKey.create())
                        .apply(ParDo.of(new DoFn<KV<String, Iterable<StudentScore>>, StudentScore>() {
                            @ProcessElement
                            public void process(@Element KV<String, Iterable<StudentScore>> scoreKV, OutputReceiver<StudentScore> receiver) {
                                Iterable<StudentScore> studentScores = scoreKV.getValue();
                                String[] keys = scoreKV.getKey().split(",");
                                StudentScore score = new StudentScore();
                                score.setDataId(SnowFlakeUtil.getSnowFlakeId());
                                score.setSchoolCode(keys[0]);
                                score.setClassCode(keys[1]);
                                score.setSubject(keys[2]);
                                score.setScore(0.00d);
                                int count = 0;
                                for (StudentScore studentScore : studentScores) {
                                    score.setSchoolName(studentScore.getSchoolName());
                                    score.setScore(score.getScore() + studentScore.getScore());
                                    count++;
                                }
                                if (count > 0) {
                                    score.setScore(score.getScore() / count);
                                }
                                receiver.output(score);
                            }
                        }));
            }
        });

        schoolClassSubjectAverage.apply(ParDo.of(new LogOutput<>("SchoolClassSubjectAverage")));

        //计算每个学校的单科平均分
        PCollection<StudentScore> schoolSubjectAverage = schoolClassSubjectAverage.apply(new PTransform<PCollection<StudentScore>, PCollection<StudentScore>>() {
            @Override
            public PCollection<StudentScore> expand(PCollection<StudentScore> input) {
                return input.apply(MapElements.via(new SimpleFunction<StudentScore, KV<String, StudentScore>>() {
                    @Override
                    public KV<String, StudentScore> apply(StudentScore input) {
                        try {
                            return KV.of(input.schoolCode + input.subject, input.clone());
                        } catch (CloneNotSupportedException e) {
                            throw new RuntimeException(e);
                        }
                    }
                })).apply(Combine.perKey(new SimpleFunction<Iterable<StudentScore>, StudentScore>() {
                    @Override
                    public StudentScore apply(Iterable<StudentScore> input) {
                        List<StudentScore> studentScores = Lists.newArrayList(input);
                        StudentScore score = null;
                        try {
                            score = studentScores.get(0).clone();
                            score.setScore(0.0d);
                            for (StudentScore studentScore : studentScores) {
                                score.setScore(score.getScore() + studentScore.getScore());
                            }
                        } catch (CloneNotSupportedException e) {
                            throw new RuntimeException(e);
                        }
                        score.setScore(score.getScore() / studentScores.size());
                        return score;
                    }
                })).apply(Values.<StudentScore>create());
            }
        });
        schoolSubjectAverage.apply(ParDo.of(new LogOutput<>("SchoolSubjectAverage")));
        pipeline.run();
    }

    private static List<StudentScore> getRandomStudentScores() {
        List<StudentScore> scores = Lists.newArrayList();
        for (long index = 0; index < 360; index++) {
            StudentScore score = new StudentScore();
            score.setDataId(index);
            score.setSchoolCode(SCHOOL_CODE[RandomUtils.nextInt(0, 2)]);
            score.setClassCode(CLASS_CODE[RandomUtils.nextInt(0, 5)]);
            score.setSubject(SUBJECT_CODE[RandomUtils.nextInt(0, 3)]);
            score.setStudentCode(STUDENT_CODE[RandomUtils.nextInt(0, 39)]);
            score.setScore(Double.valueOf(RandomUtils.nextInt(65, 100)));
            scores.add(score);
        }
        return scores;
    }


    private static List<StudentScore> getFixedStudentScores() {
        List<StudentScore> scores = Lists.newArrayList();
        Long dataId = 1l;
        for (String school : SCHOOL_CODE) {
            for (String classes : CLASS_CODE_FIXED) {
                for (String subject : SUBJECT_CODE) {
                    for (String student : STUDENT_CODE_FIXED) {
                        StudentScore score = new StudentScore();
                        score.setDataId(dataId++);
                        score.setSchoolCode(school);
                        score.setClassCode(classes);
                        score.setSubject(subject);
                        score.setStudentCode(student);
                        //score.setScore(Double.valueOf(RandomUtils.nextInt(65, 100)));
                        score.setScore(Double.valueOf(80));
                        scores.add(score);
                    }
                }
            }
        }
        return scores;
    }

    @Data
    public static class StudentScore implements Serializable, Cloneable {

        private Long dataId;

        private String studentCode;

        private String schoolCode;

        private String classCode;

        private String subject;

        private Double score;

        private String schoolName;

        @Override
        protected StudentScore clone() throws CloneNotSupportedException {
            return (StudentScore) super.clone();
        }
    }
}
