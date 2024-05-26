package cn.itcast.mr.invertedIndex;

import org.apache.commons.lang.StringUtils;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;

import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.lib.input.FileSplit;


import java.io.IOException;

public class InvertedIndexMapper extends Mapper<LongWritable, Text, Text, Text> {
    private static Text keyInfo = new Text();   //存储单词和文档名称
    private static final Text valueInfo = new Text("1");    // 存储词频,初始化为1

    /**
     * 在该方法中将K1、V1转换为K2、V2
     * key: K1行偏移量
     * value: V1行文本数据
     * context: 上下文对象
     * 输出： <MapReduce:file3 "1">
     */
    @Override
    protected void map(LongWritable key, Text value, Mapper<LongWritable, Text, Text, Text>.Context context)
            throws IOException, InterruptedException {
        String line = value.toString();
        String[] fields = StringUtils.split(line, " "); // 得到单词数组

        //得到这行数据所在的文件切片
        FileSplit fileSplit = (FileSplit) context.getInputSplit();

        //根据文件切片得到文件名
        String fileName = fileSplit.getPath().getName();

        for (String field : fields
             ) {
            // key值由单词和文件名组成，如“MapReduce:file1”
            keyInfo.set(field + ":" + fileName);
            context.write(keyInfo, valueInfo);
        }
    }
}
