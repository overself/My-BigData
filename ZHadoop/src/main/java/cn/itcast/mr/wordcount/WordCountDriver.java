package cn.itcast.mr.wordcount;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

import java.io.BufferedReader;
import java.io.FileReader;
import java.io.IOException;

public class WordCountDriver {
    public static void main(String[] args) throws IOException, ClassNotFoundException, InterruptedException {
        //通过 Job 来封装本次 MR 的相关信息
        Configuration conf = new Configuration();
        //配置MR运行模式，使用 local 表示本地模式，可以省略
        conf.set("mapreduce.framework.name","local");
        //获取 Job 运行实例
        Job wcjob = Job.getInstance(conf);
        //指定 MR Job jar运行主类
        wcjob.setJarByClass(WordCountDriver.class);
        //指定本次 MR 所有的 Mapper Combiner Reducer类
        wcjob.setMapperClass(WordCountMapper.class);
        wcjob.setCombinerClass(WordCountCombiner.class); //不指定Combiner的话也不影响结果
        wcjob.setReducerClass(WordCountReducer.class);
        //设置业务逻辑 Mapper 类的输出 key 和 value 的数据类型
        wcjob.setMapOutputKeyClass(Text.class);
        wcjob.setMapOutputValueClass(IntWritable.class);

        //设置业务逻辑 Reducer 类的输出 key 和 value 的数据类型
        wcjob.setOutputKeyClass(Text.class);
        wcjob.setOutputValueClass(IntWritable.class);

        //使用本地模式指定要处理的数据所在的位置
        FileInputFormat.setInputPaths(wcjob,"/ZHadoop/textHadoop/WordCount/input");
        //使用本地模式指定处理完成后的结果所保持的位置
        FileOutputFormat.setOutputPath(wcjob,new Path("/ZHadoop/textHadoop/WordCount/output"));

        //提交程序并且监控打印程序执行情况
        boolean res = wcjob.waitForCompletion(true);
        if (res) {
            FileReader fr = new FileReader("/ZHadoop/textHadoop/WordCount/output/part-r-00000");
            BufferedReader reader= new BufferedReader(fr);
            String str;
            while ( (str = reader.readLine()) != null )
                System.out.println(str);

            System.out.println("运行成功");
        }
        System.exit(res ? 0:1);
    }
}
