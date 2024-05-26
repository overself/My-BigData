package com.spark.project.demo.hadoop;

import lombok.extern.slf4j.Slf4j;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;
import java.util.concurrent.atomic.AtomicInteger;

@Slf4j
public class WordCountReducer extends Reducer<Text, IntWritable, Text, IntWritable> {
    //参数同Map一样，依次表示是输入键类型，输入值类型，输出键类型，输出值类型
    private IntWritable result = new IntWritable();

    @Override
    protected void reduce(Text key, Iterable<IntWritable> values, Context context) throws IOException, InterruptedException {
        AtomicInteger sum = new AtomicInteger(0);
        //for循环遍历，将得到的values值累加
        values.forEach(value -> sum.addAndGet(value.get()));
        result.set(sum.intValue());
        context.write(key, result);
    }
}
