package com.spark.project.demo.hadoop;

import lombok.extern.slf4j.Slf4j;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;
import java.util.StringTokenizer;

@Slf4j
public class WordCountMapper extends Mapper<Object, Text, Text, IntWritable> {

    //第一个Object表示输入key的类型；第二个Text表示输入value的类型；第三个Text表示输出键的类型；第四个IntWritable表示输出值的类型
    public static final IntWritable one = new IntWritable(1);
    public static Text word = new Text();

    @Override
    protected void map(Object key, Text value, Context context) throws IOException, InterruptedException {
        StringTokenizer tokenizer = new StringTokenizer(value.toString());
        //StringTokenizer是Java工具包中的一个类，用于将字符串进行拆分
        while (tokenizer.hasMoreTokens()) {
            String token = tokenizer.nextToken();
            //返回当前位置到下一个分隔符之间的字符串
            word.set(token);
            //将word存到容器中，记一个数
            context.write(word, one);
        }
    }
}
