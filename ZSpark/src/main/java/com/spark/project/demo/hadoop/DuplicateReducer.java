package com.spark.project.demo.hadoop;

import lombok.extern.slf4j.Slf4j;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;
import java.util.concurrent.atomic.AtomicInteger;

@Slf4j
public class DuplicateReducer extends Reducer<Text, Text, Text, NullWritable> {

    private AtomicInteger atomic = new AtomicInteger(1);

    //reduce将输入中的key复制到输出数据的key上，并直接输出
    public void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
        context.write(key, NullWritable.get());
    }

}
