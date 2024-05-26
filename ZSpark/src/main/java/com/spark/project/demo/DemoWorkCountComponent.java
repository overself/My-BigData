package com.spark.project.demo;

import com.spark.project.common.HdfsClient;
import com.spark.project.config.hadoop.HadoopProperties;
import com.spark.project.demo.hadoop.JobWordCountMapReduce;
import com.spark.project.demo.spark.JobWordCountSpark;
import lombok.SneakyThrows;
import lombok.extern.slf4j.Slf4j;
import org.springframework.stereotype.Component;

import javax.annotation.Nonnull;
import javax.annotation.Resource;
import java.io.IOException;
import java.util.HashMap;
import java.util.Map;

@Slf4j
@Component
public class DemoWorkCountComponent {

    @Resource
    private JobWordCountMapReduce wordCountMapReduce;

    @Resource
    private JobWordCountSpark wordCountSpark;

    @Resource
    private HdfsClient hdfsService;

    @Resource
    private HadoopProperties properties;

    @SneakyThrows
    public Map<String, Object> runWorkCount(String startType, boolean cleanUp, long size) {
        initData(startType, cleanUp, size); //生成1MB大小的文件1048576
        if ("mapreduce".equals(startType)) {
            startWordCountAnalysisByMapReduce();
        }
        if ("spark".equals(startType)) {
            startWordCountAnalysisBySpark();
        }
        return new HashMap<>();
    }

    public void initData(@Nonnull String startType, Boolean cleanUp, Long size) throws IOException {
        log.info("running at PerformanceAnalysisByWorkCount");
        TextFileGenerator generator = new TextFileGenerator();
        String dataDir = "/data";
        if (cleanUp && hdfsService.existFile("/test/"+startType+"/in")) {
            hdfsService.deleteFile("/test/"+startType+"/in");
        }
        if (size == null || size < 1) {
            size = 1l; //默认1M
        }
        String fileName = "WordCountAnalysis_" + System.currentTimeMillis() + "_" + size + "M.txt";
        String filePath = dataDir + "/" + fileName;
        generator.generateTextFile(filePath, 1024 * 1024 * size); //B-K-M：生成1MB大小的文件1048576
        hdfsService.uploadFile(filePath, "/test/"+startType+"/in/" + fileName);
    }

    @SneakyThrows
    public void startWordCountAnalysisByMapReduce() {
        log.info("doWordCountAnalysisByMapReduce start....");
        long startTime = System.currentTimeMillis();
        wordCountMapReduce.startWordCountAnalysis("/test/mapreduce/in", "/test/mapreduce/out");
        log.info("doWordCountAnalysisByMapReduce completed，cost：{} seconds", (System.currentTimeMillis() - startTime) / 1000);

    }

    @SneakyThrows
    public void startWordCountAnalysisBySpark() {
        log.info("doWordCountAnalysisBySpark start....");
        long startTime = System.currentTimeMillis();
        wordCountSpark.startWordCountAnalysis(properties.getHdfs().getUri() + "/test/spark/in", properties.getHdfs().getUri() + "/test/spark/out");
        log.info("doWordCountAnalysisBySpark completed，cost：{} seconds", (System.currentTimeMillis() - startTime) / 1000);
    }

}
