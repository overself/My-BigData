package com.beam.project.demo.stream.transform;

import com.beam.project.demo.bean.ExamScore;
import com.google.common.collect.Lists;
import lombok.SneakyThrows;
import lombok.extern.slf4j.Slf4j;
import org.apache.beam.sdk.transforms.*;
import org.apache.beam.sdk.values.KV;
import org.apache.beam.sdk.values.PCollection;

import java.math.BigDecimal;
import java.util.List;

@Slf4j
public class CalcClassSubjectAverageTransform extends PTransform<PCollection<ExamScore>, PCollection<ExamScore>> {
    @Override
    public PCollection<ExamScore> expand(PCollection<ExamScore> input) {
        PCollection<KV<String, Iterable<ExamScore>>> classSubjectGroup = input.apply(WithKeys.of(new SimpleFunction<ExamScore, String>() {
            @Override
            public String apply(ExamScore input) {
                return input.getClassSubjectKey();
            }
        })).apply(GroupByKey.create());

        return classSubjectGroup.apply("#ClassSubjectGroupSum", ParDo.of(new DoFn<KV<String, Iterable<ExamScore>>, ExamScore>() {
            @SneakyThrows
            @ProcessElement
            public void processElement(@Element KV<String, Iterable<ExamScore>> groupSubject, OutputReceiver<ExamScore> receiver) {
                String classSubjectGroupKey = groupSubject.getKey();
                List<ExamScore> examScoreList = Lists.newArrayList(groupSubject.getValue());
                if (examScoreList == null || examScoreList.size() == 0) {
                    log.warn("班级科目分组[{}]的数据集为空", classSubjectGroupKey);
                    return;
                }
                ExamScore score = examScoreList.get(0).clone();
                score.setScore(BigDecimal.ZERO);
                score.setStudentCode(null);
                examScoreList.forEach(item -> score.setScore(score.getScore().add(item.getScore())));
                //score.setScore(score.getScore().divide(BigDecimal.valueOf(examScoreList.size()), 2, RoundingMode.UP));
                //log.info("班级科目分组[{}]：平均分为：{}", classSubjectGroup, score);
                receiver.output(score);
            }
        }));
    }
}
