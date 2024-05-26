package com.spark.project.demo.hadoop;

import com.clearspring.analytics.util.Lists;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;
import java.util.Comparator;
import java.util.List;

public class WordSortReduce extends Reducer<LongWritable, Text, LongWritable, Text> {

    private LongWritable num = new LongWritable(1);

    @Override
    protected void reduce(LongWritable key, Iterable<Text> values, Reducer<LongWritable, Text, LongWritable, Text>.Context context) throws IOException, InterruptedException {
        List<String> valueList = Lists.newArrayList();
        values.forEach(v -> valueList.add(v.toString()));
        valueList.sort(Comparator.comparing(String::toString));
        for (String value : valueList) {
            // 输出排名、单词和个数
            context.write(num, new Text(value));
            num = new LongWritable(num.get() + 1);
        }
    }

}
