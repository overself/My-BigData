package cn.itcast.mr.invertedIndex;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;

public class InvertedIndexReducer extends Reducer<Text, Text, Text, Text> {
    private static Text result = new Text();

    // 输入：<MapReduce, file3:2>
    // 输出：<MapReduce, file1:1;file2:1;file3:2;>
    @Override
    protected void reduce(Text key, Iterable<Text> values, Reducer<Text, Text, Text, Text>.Context context)
            throws IOException, InterruptedException {
        // 生成文档列表
        String fileList = new String();
        for (Text value : values
             ) {
            fileList += value.toString() + ";";
        }
        result.set(fileList);
        context.write(key, result);
    }
}
