package com.beam.project.demo.stream.transform;

import com.beam.project.demo.bean.ExamScore;
import lombok.SneakyThrows;
import lombok.extern.slf4j.Slf4j;
import org.apache.beam.sdk.transforms.*;
import org.apache.beam.sdk.values.KV;
import org.apache.beam.sdk.values.PCollection;
import org.apache.beam.vendor.guava.v32_1_2_jre.com.google.common.collect.Lists;

import java.math.BigDecimal;
import java.math.RoundingMode;
import java.util.List;

@Slf4j
public class CalcSchoolSubjectAverageTransform extends PTransform<PCollection<ExamScore>, PCollection<ExamScore>> {
    @Override
    public PCollection<ExamScore> expand(PCollection<ExamScore> input) {
        PCollection<KV<String, Iterable<ExamScore>>> classSubjectGroup = input.apply(WithKeys.of(new SimpleFunction<ExamScore, String>() {
            @Override
            public String apply(ExamScore input) {
                return input.getSchoolSubjectKey();
            }
        })).apply(GroupByKey.create());

        return classSubjectGroup.apply("", ParDo.of(new DoFn<KV<String, Iterable<ExamScore>>, ExamScore>() {
            @SneakyThrows
            @ProcessElement
            public void processElement(@Element KV<String, Iterable<ExamScore>> groupSubject, OutputReceiver<ExamScore> receiver) {
                List<ExamScore> examScoreList = Lists.newArrayList(groupSubject.getValue());
                ExamScore score = examScoreList.get(0).clone();
                score.setScore(BigDecimal.ZERO);
                score.setClassCode(null);
                score.setStudentCode(null);
                examScoreList.forEach(item -> score.setScore(score.getScore().add(item.getScore())));
                //score.setScore(score.getScore().divide(BigDecimal.valueOf(examScoreList.size()), 2, RoundingMode.UP));
                //log.info("学校科目分组[{}]：平均分为：{}", groupSubject.getKey(), score);
                receiver.output(score);
            }
        }));
    }
}
