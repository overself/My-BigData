package com.beam.project.demo.basic.pipeline;

import com.beam.project.common.LogOutput;
import com.beam.project.common.SnowFlakeUtil;
import com.beam.project.demo.bean.ExamScore;
import com.beam.project.demo.bean.Subject;
import com.google.api.client.util.Lists;
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

import java.math.BigDecimal;
import java.math.RoundingMode;
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
        List<ExamScore> scores = getFixedStudentScores();

        List<Long> timestamps = Lists.newArrayList();
        for (int stimes = 0; stimes < scores.size(); stimes++) {
            timestamps.add(Duration.standardSeconds(stimes).getMillis());
        }

        // Create a PCollection from the input data with timestamps
        PCollection<ExamScore> items = pipeline.apply(Create.timestamped(scores, timestamps));

        // Create a windowed PCollection
        PCollection<ExamScore> windowedItems = items.apply(Window.into(FixedWindows.of(Duration.standardMinutes(3))));

        //每个窗口收集到的数据个数
        windowedItems.apply(ParDo.of(new LogOutput<>("Windowed Items")));
        PCollection<Long> count = windowedItems.apply("CountElements",
                Combine.globally(Count.<ExamScore>combineFn()).withoutDefaults());
        count.apply(ParDo.of(new LogOutput<>("Windowed Elements Count")));
        //对每个时间窗口的数据进行分组
        PCollection<KV<String, Iterable<ExamScore>>> windowedCountsGroup = windowedItems.apply(
                WithKeys.of(new SimpleFunction<ExamScore, String>() {
                    @Override
                    public String apply(ExamScore input) {
                        return input.getSchoolCode() + "," + input.getClassCode();
                    }
                })).apply(GroupByKey.create());
        windowedCountsGroup.apply(ParDo.of(new LogOutput<>("StudentCountsGroup")));

        //去重处理
        PCollection<ExamScore> distinctScores = windowedItems
                .apply(Distinct.withRepresentativeValueFn(new SimpleFunction<ExamScore, String>() {
                    @Override
                    public String apply(ExamScore input) {
                        return input.getSchoolCode() + "," + input.getClassCode() + "," + input.getStudentCode() + "," + input.getSubject();
                    }
                }));
        PCollection<Long> distinctCount = distinctScores.apply("DistinctCount",
                Combine.globally(Count.<ExamScore>combineFn()).withoutDefaults());
        distinctCount.apply(ParDo.of(new LogOutput<>("Distinct Elements Count")));
        //对每个时间窗口的数据进行分组
        PCollection<KV<String, Iterable<ExamScore>>> windowedDistinctGroup = distinctScores.apply(
                WithKeys.of(new SimpleFunction<ExamScore, String>() {
                    @Override
                    public String apply(ExamScore input) {
                        return input.getSchoolCode() + "," + input.getClassCode();
                    }
                })).apply(GroupByKey.create());
        windowedDistinctGroup.apply(ParDo.of(new LogOutput<>("DistinctScoresGroup")));

        //排除物理科目的统计,并给出分校区名称
        PCollection<ExamScore> mainSubjectScores = distinctScores.apply(ParDo.of(new DoFn<ExamScore, ExamScore>() {
            @ProcessElement
            public void process(@Element ExamScore score, OutputReceiver<ExamScore> receiver) throws CloneNotSupportedException {
                if (Subject.physics == score.getSubject()) {
                    return;
                }
                ExamScore scoreNew = score.clone();
                scoreNew.setSchoolName("实验中学" + score.getSchoolCode() + "分校区");
                receiver.output(scoreNew);
            }
        }));

        //计算每个班级的单科平均分
        PCollection<ExamScore> schoolClassSubjectAverage = mainSubjectScores.apply(new PTransform<PCollection<ExamScore>, PCollection<ExamScore>>() {
            @Override
            public PCollection<ExamScore> expand(PCollection<ExamScore> input) {
                return input.apply(WithKeys.of(new SimpleFunction<ExamScore, String>() {
                            @Override
                            public String apply(ExamScore input) {
                                return input.getSchoolCode() + "," + input.getClassCode() + "," + input.getSubject();
                            }
                        }))
                        .apply(GroupByKey.create())
                        .apply(ParDo.of(new DoFn<KV<String, Iterable<ExamScore>>, ExamScore>() {
                            @ProcessElement
                            public void process(@Element KV<String, Iterable<ExamScore>> scoreKV, OutputReceiver<ExamScore> receiver) {
                                Iterable<ExamScore> studentScores = scoreKV.getValue();
                                String[] keys = scoreKV.getKey().split(",");
                                ExamScore score = new ExamScore();
                                score.setDataId(SnowFlakeUtil.getSnowFlakeId());
                                score.setSchoolCode(keys[0]);
                                score.setClassCode(keys[1]);
                                score.setSubject(Subject.fromCode(keys[2]));
                                score.setScore(BigDecimal.ZERO);
                                int count = 0;
                                for (ExamScore studentScore : studentScores) {
                                    score.setSchoolName(studentScore.getSchoolName());
                                    score.setScore(score.getScore().add(studentScore.getScore()));
                                    count++;
                                }
                                if (count > 0) {
                                    score.setScore(score.getScore().divide(BigDecimal.valueOf(count), RoundingMode.UP));
                                }
                                receiver.output(score);
                            }
                        }));
            }
        });

        schoolClassSubjectAverage.apply(ParDo.of(new LogOutput<>("SchoolClassSubjectAverage")));

        //计算每个学校的单科平均分
        PCollection<ExamScore> schoolSubjectAverage = schoolClassSubjectAverage.apply(new PTransform<PCollection<ExamScore>, PCollection<ExamScore>>() {
            @Override
            public PCollection<ExamScore> expand(PCollection<ExamScore> input) {
                return input.apply(MapElements.via(new SimpleFunction<ExamScore, KV<String, ExamScore>>() {
                    @Override
                    public KV<String, ExamScore> apply(ExamScore input) {
                        try {
                            return KV.of(input.getSchoolCode() + input.getSubject(), input.clone());
                        } catch (CloneNotSupportedException e) {
                            throw new RuntimeException(e);
                        }
                    }
                })).apply(Combine.perKey(new SimpleFunction<Iterable<ExamScore>, ExamScore>() {
                    @Override
                    public ExamScore apply(Iterable<ExamScore> input) {
                        List<ExamScore> studentScores = Lists.newArrayList(input);
                        ExamScore score = null;
                        try {
                            score = studentScores.get(0).clone();
                            score.setScore(BigDecimal.ZERO);
                            for (ExamScore studentScore : studentScores) {
                                score.setScore(score.getScore().add(studentScore.getScore()));
                            }
                        } catch (CloneNotSupportedException e) {
                            throw new RuntimeException(e);
                        }
                        score.setScore(score.getScore().divide(BigDecimal.valueOf(studentScores.size()), RoundingMode.UP));
                        return score;
                    }
                })).apply(Values.<ExamScore>create());
            }
        });
        schoolSubjectAverage.apply(ParDo.of(new LogOutput<>("SchoolSubjectAverage")));
        pipeline.run();
    }

    private static List<ExamScore> getRandomStudentScores() {
        List<ExamScore> scores = Lists.newArrayList();
        for (long index = 0; index < 360; index++) {
            ExamScore score = new ExamScore();
            score.setDataId(index);
            score.setSchoolCode(SCHOOL_CODE[RandomUtils.nextInt(0, 2)]);
            score.setClassCode(CLASS_CODE[RandomUtils.nextInt(0, 5)]);
            score.setSubject(Subject.fromCode(SUBJECT_CODE[RandomUtils.nextInt(0, 3)]));
            score.setStudentCode(STUDENT_CODE[RandomUtils.nextInt(0, 39)]);
            score.setScore(BigDecimal.valueOf(RandomUtils.nextInt(65, 100)));
            scores.add(score);
        }
        return scores;
    }


    private static List<ExamScore> getFixedStudentScores() {
        List<ExamScore> scores = Lists.newArrayList();
        Long dataId = 1L;
        for (String school : SCHOOL_CODE) {
            for (String classes : CLASS_CODE_FIXED) {
                for (String subject : SUBJECT_CODE) {
                    for (String student : STUDENT_CODE_FIXED) {
                        ExamScore score = new ExamScore();
                        score.setDataId(dataId++);
                        score.setSchoolCode(school);
                        score.setClassCode(classes);
                        score.setSubject(Subject.fromCode(subject));
                        score.setStudentCode(student);
                        //score.setScore(BigDecimal.valueOf(RandomUtils.nextInt(65, 100)));
                        score.setScore(BigDecimal.valueOf(80));
                        scores.add(score);
                    }
                }
            }
        }
        return scores;
    }

}
