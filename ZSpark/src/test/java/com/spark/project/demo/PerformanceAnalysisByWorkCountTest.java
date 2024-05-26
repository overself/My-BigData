package com.spark.project.demo;

import com.spark.project.BootBaseTest;
import lombok.SneakyThrows;
import lombok.extern.slf4j.Slf4j;
import org.junit.jupiter.api.*;

import javax.annotation.Resource;


@Slf4j
@TestMethodOrder(value = MethodOrderer.OrderAnnotation.class)
public class PerformanceAnalysisByWorkCountTest extends BootBaseTest {

    @Resource
    private DemoWorkCountComponent wordCountComponent;

    @Test
    @Order(1)
    @SneakyThrows
    public void doWordCountAnalysisByMapReduce() {
        log.info("doWordCountAnalysisByMapReduce Test init");
        wordCountComponent.initData("mapreduce", false, 1024l);
        wordCountComponent.startWordCountAnalysisByMapReduce();
    }

    @Test
    @Order(2)
    @SneakyThrows
    public void doWordCountAnalysisBySpark() {
        log.info("doWordCountAnalysisBySpark Test init");
        wordCountComponent.initData("spark", false, 1024l);
        wordCountComponent.startWordCountAnalysisBySpark();
    }

}
