package cn.itcast.mr.invertedIndex;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

import java.io.BufferedReader;
import java.io.FileReader;
import java.io.IOException;

public class InvertedIndexDriver {
    public static void main(String[] args) throws IOException,
            ClassNotFoundException, InterruptedException {
        //通过 Job 来封装本次 MR 的相关信息
        Configuration conf = new Configuration();

        //获取 Job 运行实例
        Job job = Job.getInstance(conf);

        //指定 MR Job jar运行主类
        job.setJarByClass(InvertedIndexDriver.class);

        //指定本次 MR 所有的 Mapper Combiner Reducer类
        job.setMapperClass(InvertedIndexMapper.class);
        job.setCombinerClass(InvertedIndexCombiner.class);
        job.setReducerClass(InvertedIndexReducer.class);

        //设置业务逻辑 Mapper 类的输出 key 和 value 的数据类型
        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(Text.class);

        //设置业务逻辑 Reducer 类的输出 key 和 value 的数据类型
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(Text.class);

        //使用本地模式指定要处理的数据所在的位置
        FileInputFormat.setInputPaths(job, new Path("/ZHadoop/textHadoop/InvertedIndex/input"));

        //使用本地模式指定处理完成后的结果所保持的位置
        FileOutputFormat.setOutputPath(job, new Path("/ZHadoop/textHadoop/InvertedIndex/output"));

        //提交程序并且监控打印程序执行情况
        boolean res = job.waitForCompletion(true);
        if (res) {
            FileReader fr = new FileReader("/ZHadoop/textHadoop/InvertedIndex/output/part-r-00000");
            BufferedReader reader= new BufferedReader(fr);
            String str;
            while ( (str = reader.readLine()) != null )
                System.out.println(str);

            System.out.println("运行成功");
        }
        System.exit(res ? 0 : 1);
    }
}
