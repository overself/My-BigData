package com.beam.project.demo.window.gloab;

import org.apache.beam.sdk.Pipeline;
import org.apache.beam.sdk.io.TextIO;
import org.apache.beam.sdk.io.kafka.KafkaIO;
import org.apache.beam.sdk.options.PipelineOptionsFactory;
import org.apache.beam.sdk.transforms.*;
import org.apache.beam.sdk.transforms.windowing.*;
import org.apache.beam.sdk.values.KV;
import org.apache.beam.sdk.values.PCollection;
import org.apache.beam.sdk.values.TypeDescriptor;
import org.apache.beam.sdk.values.TypeDescriptors;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.checkerframework.checker.initialization.qual.Initialized;
import org.checkerframework.checker.nullness.qual.NonNull;
import org.checkerframework.checker.nullness.qual.UnknownKeyFor;
import org.joda.time.Duration;

import java.util.List;


public class SchoolPipelineSum {

    public static void main(String[] args) {
        long startTime = System.currentTimeMillis();

        Pipeline pipeline = Pipeline.create(PipelineOptionsFactory.fromArgs(args).withValidation().create());

        // Step 1: Read from Kafka
        PCollection<String> kafkaInput = pipeline
                .apply(KafkaIO.<String, String>read()
                        .withBootstrapServers("kafka-bootstrap-server:9092")
                        .withTopic("school-topic")
                        .withKeyDeserializer(StringDeserializer.class)
                        .withValueDeserializer(StringDeserializer.class)
                        .withoutMetadata())
                .apply(MapElements.into(TypeDescriptors.strings())
                        .via((KV<String, String> kafkaRecord) -> kafkaRecord.getValue()));

        // Step 2: Deduplicate School records with windowing
        PCollection<School> uniqueSchools = kafkaInput
                .apply(Window.<String>into(FixedWindows.of(Duration.standardMinutes(5)))
                        .triggering(AfterProcessingTime.pastFirstElementInPane()
                                .plusDelayOf(Duration.standardMinutes(5)))
                        .withAllowedLateness(Duration.ZERO)
                        .discardingFiredPanes())
                .apply(MapElements.into(TypeDescriptor.of(School.class))
                        .via((String json) -> parseSchoolJson(json)))
                .apply(Distinct.withRepresentativeValueFn((School school) -> school.code));

        // Step 3: Get Exam Scores from DB
        PCollection<ExamScore> examScores = uniqueSchools
                .apply(ParDo.of(new GetExamScoresFromDB()));

        // Step 4: Compute ClassSubjectScore with custom CombineFn
        PCollection<KV<String, Double>> classSubjectScores = examScores
                .apply(MapElements.into(TypeDescriptors.kvs(TypeDescriptors.strings(), TypeDescriptor.of(ExamScore.class)))
                        .via((ExamScore score) -> {
                            assert score != null;
                            return KV.of(score.schoolCode + "_" + score.classCode + "_" + score.subject, score);
                        }))
                .apply(Combine.perKey(new SumScores()));

        // Step 5: Compute SchoolSubjectScore with custom logic
        PCollection<KV<String, Double>> schoolSubjectScores = classSubjectScores
                .apply(ParDo.of(new DoFn<KV<String, Double>, KV<String, Double>>() {
                    @ProcessElement
                    public void processElement(ProcessContext c) {
                        KV<String, Double> classSubjectScore = c.element();
                        String[] keys = classSubjectScore.getKey().split("_");
                        String schoolCode = keys[0];
                        String subject = keys[2];
                        c.output(KV.of(schoolCode + "_" + subject, classSubjectScore.getValue()));
                    }
                }))
                .apply(Combine.perKey(new Combine.CombineFn<Double, Double, Double>() {

                    @Override
                    public Double createAccumulator() {
                        return 0.0;
                    }

                    @Override
                    public Double addInput(Double mutableAccumulator, Double input) {
                        return mutableAccumulator + input;
                    }

                    @Override
                    public Double mergeAccumulators(@UnknownKeyFor @NonNull @Initialized Iterable<Double> accumulators) {
                        double sum = 0.0;
                        for (Double accum : accumulators) {
                            sum += accum;
                        }
                        return sum;
                    }

                    @Override
                    public Double extractOutput(Double accumulator) {
                        return accumulator;
                    }
                }));

        // Step 6: Output results to file
        schoolSubjectScores
                .apply(MapElements.into(TypeDescriptors.strings())
                        .via((KV<String, Double> schoolSubjectScore) -> formatResult(schoolSubjectScore)))
                .apply(Window.into(FixedWindows.of(Duration.standardMinutes(5))))
                .apply(TextIO.write().to("output/school_subject_scores").withWindowedWrites().withNumShards(1));

        pipeline.run().waitUntilFinish();

        long endTime = System.currentTimeMillis();
        System.out.println("Total processing time: " + (endTime - startTime) + " milliseconds");
    }

    private static School parseSchoolJson(String json) {
        // Implement JSON parsing logic to convert JSON string to School object
        // ...
        return new School();
    }

    private static String formatResult(KV<String, Double> schoolSubjectScore) {
        String[] keys = schoolSubjectScore.getKey().split("_");
        String schooleCode = keys[0];
        String subject = keys[1];
        Double score = schoolSubjectScore.getValue();
        return String.format("SchoolCode: %s, Subject: %s, TotalScore: %f", schooleCode, subject, score);
    }

    static class GetExamScoresFromDB extends DoFn<School, ExamScore> {
        @ProcessElement
        public void processElement(ProcessContext c) {
            School school = c.element();
            List<ExamScore> examScores = getExamScoresFromDatabase(school.code);
            for (ExamScore score : examScores) {
                c.output(score);
            }
        }

        private List<ExamScore> getExamScoresFromDatabase(String schoolCode) {
            // Implement the logic to fetch exam scores from the database
            // ...
            return List.of();
        }
    }

    static class School {
        String code;
        String name;
    }

    static class ExamScore {
        String schoolCode;
        String classCode;
        String subject;
        String studentCode;
        Double score;
    }

    // Custom CombineFn to sum scores
    public static class SumScores extends Combine.CombineFn<ExamScore, Double, Double> {
        @Override
        public Double createAccumulator() {
            return 0.0;
        }

        @Override
        public Double addInput(Double accumulator, ExamScore input) {
            return accumulator + input.score;
        }

        @Override
        public Double mergeAccumulators(Iterable<Double> accumulators) {
            double sum = 0.0;
            for (Double accum : accumulators) {
                sum += accum;
            }
            return sum;
        }

        @Override
        public Double extractOutput(Double accumulator) {
            return accumulator;
        }
    }
}
