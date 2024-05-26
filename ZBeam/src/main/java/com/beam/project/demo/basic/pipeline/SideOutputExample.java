package com.beam.project.demo.basic.pipeline;

import com.beam.project.common.LogOutput;
import com.beam.project.demo.bean.ClassInfo;
import com.beam.project.demo.bean.GeneratorUtil;
import com.beam.project.demo.bean.Student;
import com.beam.project.demo.basic.transform.ClassNameFullForStudentDoFn;
import com.google.api.client.util.Lists;
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
import org.apache.beam.sdk.values.*;
import org.apache.commons.lang3.StringUtils;

import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

@Slf4j
public class SideOutputExample {

    public static void main(String[] args) {
        PipelineOptions options = PipelineOptionsFactory.create();
        options.setRunner(DirectRunner.class);
        Pipeline pipeline = Pipeline.create(options);

        Map<String, ClassInfo> classDataMap = GeneratorUtil.getClassInfos().stream()
                .collect(Collectors.toMap(ClassInfo::getCode, o -> o));
        PCollectionView<Map<String, ClassInfo>> sideClassData = pipeline
                .apply("CreateClassInfo", Create.of(classDataMap)
                        .withCoder(KvCoder.of(StringUtf8Coder.of(), SerializableCoder.of(ClassInfo.class))))
                .apply("ToView", View.asMap());

        PCollection<Student> studentData = pipeline.apply(
                Create.of(GeneratorUtil.getAllStudents()).withCoder(SerializableCoder.of(Student.class)));

        PCollection<Student> studentFullDataList = studentData.apply("UpdateClassName",
                ParDo.of(new ClassNameFullForStudentDoFn(sideClassData))
                        .withSideInput("sideClassData", sideClassData));
        studentFullDataList.apply(ParDo.of(new LogOutput<>("StudentPrint")));

        //找出没有分配班，或者分配班级错误的学生
        TupleTag<Student> noClassTag = new TupleTag<>("noClass") {};
        List<TupleTag<?>> classTagList = Lists.newArrayList();
        GeneratorUtil.getClassInfos().forEach(item -> classTagList.add(new TupleTag<>(item.getCode()) {}));
        //同时将已分配班级的同学，按照班级分开
        TupleTagList tupleTagLists = TupleTagList.of(classTagList);
        PCollectionTuple classFilterTuple = studentFullDataList.apply("ClassFilter", ParDo.of(new DoFn<Student, Student>() {
            @ProcessElement
            public void processElement(@Element Student student, ProcessContext context) {
                if (StringUtils.isBlank(student.getClassName())) {
                    context.output(noClassTag, student);
                } else {
                    for (TupleTag<?> tupleTag : tupleTagLists.getAll()) {
                        if (tupleTag.getId().equals(student.getClassCode())) {
                            context.output((TupleTag<Student>) tupleTag, student);
                            break;
                        }
                    }
                }
            }
        }).withOutputTags(noClassTag, tupleTagLists));

        Map<TupleTag<?>, PCollection<?>> tupleMap = classFilterTuple.getAll();
        tupleMap.forEach((key, value) -> {
            TupleTag<Student> studentTuple = (TupleTag<Student>) key;
            PCollection<Student> studentPc = (PCollection<Student>) value;
            studentPc.setCoder(SerializableCoder.of(Student.class))
                    .apply(ParDo.of(new LogOutput<>(studentTuple.getId())));
        });

        PipelineResult.State state = pipeline.run().waitUntilFinish();
        log.info("SideMapData pipeline run {}", state);
    }

}
