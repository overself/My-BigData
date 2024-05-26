package com.beam.project.demo.basic.pipeline;

import com.beam.project.common.LogOutput;
import com.beam.project.demo.bean.ClassInfo;
import com.beam.project.demo.bean.GeneratorUtil;
import com.beam.project.demo.bean.Student;
import lombok.extern.slf4j.Slf4j;
import org.apache.beam.runners.direct.DirectRunner;
import org.apache.beam.sdk.Pipeline;
import org.apache.beam.sdk.PipelineResult;
import org.apache.beam.sdk.coders.KvCoder;
import org.apache.beam.sdk.coders.SerializableCoder;
import org.apache.beam.sdk.coders.StringUtf8Coder;
import org.apache.beam.sdk.options.PipelineOptions;
import org.apache.beam.sdk.options.PipelineOptionsFactory;
import org.apache.beam.sdk.transforms.Create;
import org.apache.beam.sdk.transforms.DoFn;
import org.apache.beam.sdk.transforms.ParDo;
import org.apache.beam.sdk.transforms.View;
import org.apache.beam.sdk.values.PCollection;
import org.apache.beam.sdk.values.PCollectionView;

import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

@Slf4j
public class SideInputExample {

    public static void main(String[] args) {
        SideInputExample.SideListData(args);
        SideInputExample.SideMapData(args);
    }

    public static void SideListData(String[] args) {
        PipelineOptions options = PipelineOptionsFactory.create();
        options.setRunner(DirectRunner.class);
        Pipeline pipeline = Pipeline.create(options);

        PCollectionView<List<ClassInfo>> sideClassData = pipeline
                .apply("CreateClassInfo", Create.of(GeneratorUtil.getClassInfos())
                        .withCoder(SerializableCoder.of(ClassInfo.class)))
                .apply("ToView", View.asList());

        PCollection<Student> studentData = pipeline.apply(Create.of(GeneratorUtil.getClassStudents())
                .withCoder(SerializableCoder.of(Student.class)));

        PCollection<Student> studentFullDataList = studentData
                .apply("UpdateClassName", ParDo.of(new DoFn<Student, Student>() {
                    @ProcessElement
                    public void processElement(ProcessContext c) throws CloneNotSupportedException {
                        Student student = c.element();
                        Student output = student.clone();
                        List<ClassInfo> classInfos = c.sideInput(sideClassData);
                        classInfos.forEach(classInfo -> {
                            if (output.getClassCode().equals(classInfo.getCode())) {
                                output.setClassName(classInfo.getClassName());
                            }
                        });
                        c.output(output);
                    }
                }).withSideInput("sideClassData", sideClassData));
        studentFullDataList.apply(ParDo.of(new LogOutput("StudentPrint")));

        PipelineResult.State state = pipeline.run().waitUntilFinish();
        log.info("SideListData pipeline run {}", state);
    }

    public static void SideMapData(String[] args) {
        PipelineOptions options = PipelineOptionsFactory.create();
        options.setRunner(DirectRunner.class);
        Pipeline pipeline = Pipeline.create(options);

        Map<String, ClassInfo> classDataMap = GeneratorUtil.getClassInfos()
                .stream().collect(Collectors.toMap(ClassInfo::getCode, o -> o));
        PCollectionView<Map<String, ClassInfo>> sideClassData = pipeline.apply("CreateClassInfo",
                        Create.of(classDataMap).withCoder(KvCoder.of(StringUtf8Coder.of(), SerializableCoder.of(ClassInfo.class))))
                .apply("ToView", View.asMap());

        PCollection<Student> studentData = pipeline.apply(Create.of(GeneratorUtil.getClassStudents())
                .withCoder(SerializableCoder.of(Student.class)));

        PCollection<Student> studentFullDataList = studentData
                .apply("UpdateClassName", ParDo.of(new DoFn<Student, Student>() {
                    @ProcessElement
                    public void processElement(ProcessContext c) throws CloneNotSupportedException {
                        Student student = c.element();
                        Student output = student.clone();
                        Map<String, ClassInfo> classInfos = c.sideInput(sideClassData);
                        output.setClassName(classInfos.get(output.getClassCode()).getClassName());
                        c.output(output);
                    }
                }).withSideInput("sideClassData", sideClassData));
        studentFullDataList.apply(ParDo.of(new LogOutput("StudentPrint")));

        PipelineResult.State state = pipeline.run().waitUntilFinish();
        log.info("SideMapData pipeline run {}", state);
    }

}
