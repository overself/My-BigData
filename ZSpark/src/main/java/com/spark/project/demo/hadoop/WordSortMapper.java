package com.spark.project.demo.hadoop;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;
import java.util.Arrays;
import java.util.List;

public class WordSortMapper extends Mapper<LongWritable, Text, LongWritable, Text> {
    @Override
    protected void map(LongWritable key, Text value, Mapper<LongWritable, Text, LongWritable, Text>.Context context) throws IOException, InterruptedException {
        String line = value.toString();
        if (line == null || "".equals(line)) {
            return;
        }
        List<String> args = Arrays.asList(line.split(","));
        if (args.size() == 2) {
            String word = args.get(0);
            long  count = Long.valueOf(args.get(1).trim());
            // 输出键值对，其中键为个数的负值，值为单词和个数的组合
            context.write(new LongWritable(-count), new Text(word + "\t" + count));

        }
    }
}
