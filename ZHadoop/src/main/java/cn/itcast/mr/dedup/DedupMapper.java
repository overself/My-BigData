package cn.itcast.mr.dedup;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;

/*
使用MapReduce默认机制对key自动去重
 */
public class DedupMapper extends Mapper<LongWritable, Text, Text, NullWritable> {
    private static Text field = new Text();

    // 输入：<0,2018-3-3 c><11,2018-3-4 d>
    @Override
    protected void map(LongWritable key, Text value, Mapper<LongWritable, Text, Text, NullWritable>.Context context)
            throws IOException, InterruptedException {

        //直接将输入的 value 值当作 key 值输出，NullWritable.get() 方法设置空置
        field = value;
        context.write(field,NullWritable.get());
    }
    // 输出形式：<2018-3-3 c,null> <2018-3-4 d,null>
}
