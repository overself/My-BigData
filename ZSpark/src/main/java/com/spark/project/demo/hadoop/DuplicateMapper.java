package com.spark.project.demo.hadoop;

import lombok.extern.slf4j.Slf4j;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;

@Slf4j
public class DuplicateMapper extends Mapper<Object, Text, Text, NullWritable> {

    private Text line = new Text();

    // map将输入中的value复制到输出数据的key上，并直接输出
    public void map(Object key, Text value, Context context) throws IOException, InterruptedException {
        line.set(value.toString());
        context.write(line, NullWritable.get());
    }

}
