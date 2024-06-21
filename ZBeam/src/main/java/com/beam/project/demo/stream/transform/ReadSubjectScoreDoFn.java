package com.beam.project.demo.stream.transform;

import com.beam.project.demo.bean.ExamScore;
import com.beam.project.demo.bean.SchoolClass;
import com.beam.project.demo.stream.GeneratorScore;
import org.apache.beam.sdk.transforms.DoFn;

import java.util.List;

public class ReadSubjectScoreDoFn extends DoFn<SchoolClass, ExamScore> {
    @ProcessElement
    public void processElement(ProcessContext c) {
        SchoolClass schoolClass = c.element();
        List<ExamScore> scores = GeneratorScore.getSubjectScore(schoolClass);
        scores.forEach(score -> c.output(score));
    }
}
