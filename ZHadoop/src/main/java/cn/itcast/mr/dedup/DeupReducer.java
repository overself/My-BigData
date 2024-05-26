package cn.itcast.mr.dedup;

import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;

public class DeupReducer extends Reducer<Text, NullWritable, Text, NullWritable> {
    // <2018-3-3 c,null> <2018-3-4 d,null><2018-3-4 d,null>
    @Override
    protected void reduce(Text key, Iterable<NullWritable> values, Reducer<Text, NullWritable, Text, NullWritable>.Context context)
            throws IOException, InterruptedException {

        //直接将输入的 key 值当作key值输出，NullWritable.get() 方法设置空置
        context.write(key, NullWritable.get());
    }
}
