package com.spark.project.demo.hadoop;

import lombok.extern.slf4j.Slf4j;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.*;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.springframework.stereotype.Component;

import javax.annotation.Resource;

@Slf4j
@Component
public class JobWordCountMapReduce {

    @Resource
    private Configuration configuration;

    @Resource
    private FileSystem fileSystem;

    /**
     * https://blog.csdn.net/u011109589/category_7814168.html
     * <p>
     * https://blog.csdn.net/qq_30131391/article/details/106544004
     * https://blog.csdn.net/kwu_ganymede/article/details/50475788
     *
     * @return
     * @throws Exception
     */
    public boolean doWordDuplicate(String input, String output) throws Exception {
        Job job = Job.getInstance(new Configuration(configuration), "duplicate removal");
        job.setJar("target/SparkDemo-0.0.1-SNAPSHOT.jar");
        job.setJarByClass(JobWordCountMapReduce.class);
        job.setMapperClass(DuplicateMapper.class);
        job.setCombinerClass(DuplicateReducer.class);
        job.setReducerClass(DuplicateReducer.class);

        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(NullWritable.class);

        Path in = new Path(input);
        Path out = new Path(output);
        if (fileSystem.exists(out)) {
            fileSystem.delete(out, true);
        }
        FileInputFormat.addInputPath(job, in);
        FileOutputFormat.setOutputPath(job, out);
        boolean result = job.waitForCompletion(true);
        log.info("====duplicate====:{}", result);
        return result;
    }

    public boolean doWordCount(String input, String output) throws Exception {
        Configuration config = new Configuration(configuration);
        config.set("mapred.textoutputformat.separator", ",");
        Job job = Job.getInstance(config, "word count");
        job.setJar("target/SparkDemo-0.0.1-SNAPSHOT.jar");
        job.setJarByClass(JobWordCountMapReduce.class);
        job.setMapperClass(WordCountMapper.class);
        job.setCombinerClass(WordCountReducer.class);
        job.setReducerClass(WordCountReducer.class);

        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(IntWritable.class);

        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(Text.class);

        Path in = new Path(input);
        Path out = new Path(output);
        if (fileSystem.exists(out)) {
            fileSystem.delete(out, true);
        }
        FileInputFormat.addInputPath(job, in);
        FileOutputFormat.setOutputPath(job, out);
        boolean result = job.waitForCompletion(true);
        if (result) {
            fileSystem.delete(in, true);
        }
        log.info("====doWordCount====:{}", result);
        return result;
    }

    public boolean doWordSort(String input, String output) throws Exception {
        Job job = Job.getInstance(new Configuration(configuration), "word sort");

        job.setJar("target/SparkDemo-0.0.1-SNAPSHOT.jar");
        job.setJarByClass(JobWordCountMapReduce.class);
        job.setMapperClass(WordSortMapper.class);
        job.setCombinerClass(WordSortReduce.class);
        job.setReducerClass(WordSortReduce.class);

        job.setMapOutputKeyClass(LongWritable.class);
        job.setMapOutputValueClass(Text.class);

        job.setOutputKeyClass(LongWritable.class);
        job.setOutputValueClass(Text.class);

        Path in = new Path(input);
        Path out = new Path(output);
        if (fileSystem.exists(out)) {
            fileSystem.delete(out, true);
        }
        FileInputFormat.addInputPath(job, in);
        FileOutputFormat.setOutputPath(job, out);
        boolean result = job.waitForCompletion(true);
        if (result) {
            fileSystem.delete(in, true);
        }
        log.info("====doWordSort====:{}", result);
        return result;
    }

    public void startWordCountAnalysis(String input, String output) throws Exception {
        log.info("doWordCountAnalysisByMapReduce start....");
        //去重处理
        log.info("去重处理启动，处理中....");
        long subStartTime = System.currentTimeMillis();
        this.doWordDuplicate(input, "/test/mapreduce/out-duplicate");
        log.info("去重处理结束，该阶段用时：{}秒", (System.currentTimeMillis() - subStartTime) / 1000);
        //统计处理
        log.info("统计处理启动，处理中....");
        subStartTime = System.currentTimeMillis();
        this.doWordCount("/test/mapreduce/out-duplicate", "/test/mapreduce/out-count");
        log.info("统计处理结束，该阶段用时：{}秒", (System.currentTimeMillis() - subStartTime) / 1000);
        //排序处理
        log.info("排序处理启动，处理中....");
        subStartTime = System.currentTimeMillis();
        this.doWordSort("/test/mapreduce/out-count", output);
        log.info("排序处理结束，该阶段用时：{}秒", (System.currentTimeMillis() - subStartTime) / 1000);
    }

}
