package com.beam.project.demo.stream.transform;

import com.beam.project.demo.bean.*;
import com.beam.project.demo.stream.GeneratorScore;
import lombok.SneakyThrows;
import lombok.extern.slf4j.Slf4j;
import org.apache.beam.sdk.transforms.DoFn;
import org.apache.commons.lang3.RandomUtils;

import java.util.concurrent.atomic.AtomicInteger;

@Slf4j
public class GeneratorSchoolClassDoFn extends DoFn<Object, SchoolClass> {

    static AtomicInteger atomicInteger = new AtomicInteger(1);



    @SneakyThrows
    @ProcessElement
    public void processElement(ProcessContext c) {
        // 随机测试
        SchoolClass schoolClass = GeneratorScore.getRandomSchoolClass();
        c.output(schoolClass);
        log.info("发送第{}条数据：{}", atomicInteger.getAndIncrement(), schoolClass.getSchoolClassKey());
        // 固定测试
        /*
        SchoolClass schoolClass = new SchoolClass();
        schoolClass.setClassCode("SC01");
        schoolClass.setSchoolCode("CL01");
        c.output(schoolClass);
        log.info("发送第{}条数据：{}", atomicInteger.getAndIncrement(), schoolClass.getSchoolClassKey());
        SchoolClass schoolClass2 = new SchoolClass();
        schoolClass2.setClassCode("SC02");
        schoolClass2.setSchoolCode("CL02");
        c.output(schoolClass2);
        log.info("发送第{}条数据：{}", atomicInteger.getAndIncrement(), schoolClass2.getSchoolClassKey());
        SchoolClass schoolClass3 = new SchoolClass();
        schoolClass3.setClassCode("SC03");
        schoolClass3.setSchoolCode("CL03");
        c.output(schoolClass3);
        log.info("发送第{}条数据：{}", atomicInteger.getAndIncrement(), schoolClass3.getSchoolClassKey());*/
    }
}
