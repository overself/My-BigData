package cn.itcast.mr.invertedIndex;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;

/*
对每个文档中的单词进行词频统计，转换<key,value>输出形式
 */
public class InvertedIndexCombiner extends Reducer<Text, Text, Text, Text> {
    private static Text info = new Text();
    // 输入： <MapReduce:file3 {1,1,...}>
    // 输出：<MapReduce file3:2>
    @Override
    protected void reduce(Text key, Iterable<Text> values, Reducer<Text, Text, Text, Text>.Context context)
            throws IOException, InterruptedException {
        int sum = 0;    //统计词频

        //遍历一组迭代器，把每一个数量1累加起来构成了单词出现的总次数
        for (Text value : values
             ) {
            sum += Integer.parseInt(value.toString());
        }
        int splitIndex = key.toString().indexOf(":");
        // 重新设置 value 值由文件名和词频组成
        info.set(key.toString().substring(splitIndex + 1) + ":" + sum);

        // 重新设置 key 值为单词
        key.set(key.toString().substring(0, splitIndex));

        //向上下文context写入<k3,v3>
        context.write(key, info);
    }
}
