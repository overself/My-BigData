package com.beam.project.demo.basic.pipeline;

import com.beam.project.common.FlinkOptions;
import com.beam.project.common.LogOutput;
import com.beam.project.demo.bean.Student;
import com.google.api.client.util.Lists;
import lombok.extern.slf4j.Slf4j;
import org.apache.beam.runners.flink.FlinkRunner;
import org.apache.beam.sdk.Pipeline;
import org.apache.beam.sdk.PipelineResult;
import org.apache.beam.sdk.coders.Coder;
import org.apache.beam.sdk.coders.SerializableCoder;
import org.apache.beam.sdk.options.PipelineOptions;
import org.apache.beam.sdk.options.PipelineOptionsFactory;
import org.apache.beam.sdk.transforms.Create;
import org.apache.beam.sdk.transforms.Flatten;
import org.apache.beam.sdk.transforms.ParDo;
import org.apache.beam.sdk.values.PCollection;
import org.apache.beam.sdk.values.PCollectionList;

import java.util.List;

@Slf4j
public class PCollectionMergedExample {

    public static void main(String[] args) {
        String[] runArgs = new String[]{};
        FlinkOptions options = PipelineOptionsFactory.fromArgs(runArgs).create().as(FlinkOptions.class);
        options.setFlinkMaster("[local]");
        options.setRunner(FlinkRunner.class);
        Pipeline pipeline = Pipeline.create(options);
        List<Student> studentList1 = Lists.newArrayList();
        studentList1.add(new Student("S001", "张一", "C001"));
        studentList1.add(new Student("S002", "张二", "C001"));

        List<Student> studentList2 = Lists.newArrayList();
        studentList2.add(new Student("S003", "张三", "C002"));
        studentList2.add(new Student("S004", "王五", "C001"));

        List<Student> studentList3 = Lists.newArrayList();
        studentList3.add(new Student("S005", "王莽", "C002"));
        studentList3.add(new Student("S006", "李司", "C003"));

        Coder<Student> coder = SerializableCoder.of(Student.class);
        PCollectionList<Student> pCollectionList = PCollectionList
                .of(pipeline.apply(Create.of(studentList1).withCoder(coder)))
                .and(pipeline.apply(Create.of(studentList2).withCoder(coder)))
                .and(pipeline.apply(Create.of(studentList3).withCoder(coder)));
        PCollection<Student> mergedPC = pCollectionList.apply(Flatten.pCollections());
        mergedPC.apply(ParDo.of(new LogOutput<>("AllStudent")));

        PipelineResult.State state = pipeline.run().waitUntilFinish();
        log.info("PipelineResult.State: {}", state);
    }
}
