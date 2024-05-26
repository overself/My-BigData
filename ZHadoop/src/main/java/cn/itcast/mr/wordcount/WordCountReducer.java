package cn.itcast.mr.wordcount;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;

public class WordCountReducer extends Reducer<Text, IntWritable, Text, IntWritable> {
    @Override
    protected void reduce(Text key, Iterable<IntWritable> values, Reducer<Text, IntWritable, Text, IntWritable>.Context context) throws IOException, InterruptedException {
        //定义一个计数器
        int count = 0;

        //遍历一组迭代器，把每一个数量1累加起来构成了单词出现的总次数
        for (IntWritable iw : values
             ) {
            count +=iw.get();
        }

        //向上下文context写入<k3,v3>
        context.write(key, new IntWritable(count));
    }
}
