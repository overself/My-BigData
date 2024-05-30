package com.beam.project.demo.kafka.transform;

import com.beam.project.demo.bean.ExamScore;
import com.beam.project.demo.bean.GeneratorUtil;
import com.beam.project.demo.bean.Student;
import com.beam.project.demo.bean.Subject;
import com.google.common.collect.Lists;
import org.apache.beam.sdk.transforms.DoFn;
import org.apache.commons.lang3.RandomUtils;

import java.util.List;
import java.util.Random;

public class RandomScoreGeneratorFn extends DoFn<Object, ExamScore> {

    private static final double MAX_SCORE = 100;

    private static final double MIN_SCORE = 60;

    private Subject subject;

    public RandomScoreGeneratorFn(Subject subject) {
        this.subject = subject;
    }

    @ProcessElement
    public void processElement(ProcessContext c) {
        generate(this.subject).forEach(item -> c.output(item));
    }

    public List<ExamScore> generate(Subject subject) {
        List<ExamScore> scores = Lists.newArrayList();
        List<Student> students = GeneratorUtil.getClassStudents(1, 300);
        for (Student student : students) {
            ExamScore examScore = new ExamScore();
            examScore.setStudentCode(student.getCode());
            examScore.setSubject(subject);
            examScore.setScore(RandomUtils.nextDouble(MIN_SCORE, MAX_SCORE));
            scores.add(examScore);
        }
        return scores;
    }
}
