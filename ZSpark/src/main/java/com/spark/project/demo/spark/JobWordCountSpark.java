package com.spark.project.demo.spark;

import lombok.extern.slf4j.Slf4j;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.springframework.stereotype.Component;
import scala.Tuple2;

import javax.annotation.Resource;
import java.io.IOException;
import java.util.Arrays;
import java.util.concurrent.atomic.AtomicInteger;

@Slf4j
@Component
public class JobWordCountSpark {

    @Resource
    private JavaSparkContext sparkContext;

    @Resource
    private FileSystem fileSystem;

    public void startWordCountAnalysis(String input, String output) throws IOException {
        sparkContext.getConf().setAppName("JobWordCountSpark");
        //sparkContext.addJar("D:\\Jdev\\bigdata\\SparkTest\\target\\spark-app-0.0.1-SNAPSHOT.jar");
        sparkContext.addJar("target/SparkDemo-0.0.1-SNAPSHOT.jar");
        JavaRDD<String> linesRDD = sparkContext.textFile(input);
        //去重处理
        log.info("去重处理启动，处理中....");
        long subStartTime = System.currentTimeMillis();
        linesRDD = linesRDD.distinct();
        /*linesRDD.collect().forEach(line ->{
            log.info("distinct=====:{}",line);
        });*/
        log.info("去重处理结束，该阶段用时：{}毫秒", (System.currentTimeMillis() - subStartTime));
        //统计处理
        log.info("统计处理启动，处理中....");
        subStartTime = System.currentTimeMillis();
        //每个字符串以空格切割
        JavaRDD<String> wordsRDD = linesRDD.flatMap(line -> Arrays.asList(line.split(" ")).iterator());
        //将单词和一组合在一起:
        JavaPairRDD<String, Integer> wordPairRDD = wordsRDD.mapToPair(w -> new Tuple2<>(w, 1));
        //单词出现的次数聚合 CUM(次数)
        JavaPairRDD<String, Integer> reducedRDD = wordPairRDD.reduceByKey((m, n) -> m + n);
        /*reducedRDD.collect().forEach(line ->{
            log.info("reducedRDD=====:{}",line);
        });*/
        log.info("统计处理结束，该阶段用时：{}毫秒", (System.currentTimeMillis() - subStartTime));
        log.info("排序处理启动，处理中....");
        subStartTime = System.currentTimeMillis();
        //调换单词与次数的数据列顺序：次数 单词
        JavaPairRDD<Integer, String> swapedRDD = reducedRDD.mapToPair(tp -> tp.swap());
        //按照出现的次数进行降序排序
        JavaPairRDD<Integer, String> sorted = swapedRDD.sortByKey(false);
        //调换单词与次数的数据列顺序：单词 次数
        JavaPairRDD<String, Integer> resultPairRDD = sorted.mapToPair(tp -> tp.swap());
        //格式化输出结果：排名 单词 次数
        AtomicInteger sum = new AtomicInteger(1);
        JavaRDD<String> resultRDD = resultPairRDD.map(tuple -> {
            String record = sum.getAndIncrement() + " " + tuple._1 + " " + tuple._2;
            return record;
        });
        /*resultRDD.collect().forEach(line ->{
            log.info("resultRDD=====:{}",line);
        });*/
        log.info("排序处理结束，该阶段用时：{}秒", (System.currentTimeMillis() - subStartTime) / 1000);
        Path out = new Path(output);
        if (fileSystem.exists(out)) {
            fileSystem.delete(out, true);
        }
        resultRDD.coalesce(1).saveAsTextFile(output);
        sparkContext.close();

    }
}
