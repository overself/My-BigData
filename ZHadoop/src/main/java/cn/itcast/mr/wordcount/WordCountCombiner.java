package cn.itcast.mr.wordcount;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;

public class WordCountCombiner extends Reducer<Text, IntWritable, Text, IntWritable> {
    @Override
    protected void reduce(Text key, Iterable<IntWritable> values, Context context) throws IOException, InterruptedException {
        //局部汇总
        //定义一个计数器
        int count = 0;

        //遍历一组迭代器，把每一个数量1累加起来构成了单词出现的总次数
        for (IntWritable v : values
             ) {
            count += v.get();
        }

        //向上下文context写入<k3,v3>
        context.write(key, new IntWritable(count));
    }
}
