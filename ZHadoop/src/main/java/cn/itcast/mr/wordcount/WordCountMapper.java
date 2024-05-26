package cn.itcast.mr.wordcount;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;

public class WordCountMapper extends Mapper<LongWritable, Text, Text, IntWritable> {
    @Override
    protected void map(LongWritable key, Text value, Mapper<LongWritable, Text, Text, IntWritable>.Context context) throws IOException, InterruptedException {
        //接收传入进来的一行文本，并转换成String类型
        String line = value.toString();

        //将这行内容按分隔符空格切割成单词，保存在String数组中
        String[] words = line.split(" ");

        //遍历数组，产生<K2,V2>键值对，形式为<单词,1>
        for (String word : words
             ) {
            //使用context，把map阶段处理的数据发送给reduce阶段作为输入数据
            context.write(new Text(word), new IntWritable(1));
        }
    }
}
