package cn.itcast.mr.topN;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

import java.io.BufferedReader;
import java.io.FileReader;

public class TopNDriver {
    public static void main(String[] args) throws Exception{
        //通过 Job 来封装本次 MR 的相关信息
        Configuration conf = new Configuration();

        //获取 Job 运行实例
        Job job = Job.getInstance(conf);

        //指定 MR Job jar运行主类
        job.setJarByClass(TopNDriver.class);

        //指定本次 MR 所有的 Mapper Reducer类
        job.setMapperClass(TopNMapper.class);
        job.setReducerClass(TopNReducer.class);
        job.setNumReduceTasks(1);

        //map 阶段的输出 key
        job.setMapOutputKeyClass(NullWritable.class);

        //map 阶段的输出 value
        job.setMapOutputValueClass(IntWritable.class);

        //设置业务逻辑 Reducer 类的输出 key 和 value 的数据类型
        job.setOutputKeyClass(NullWritable.class);
        job.setOutputValueClass(IntWritable.class);

        //使用本地模式指定要处理的数据所在的位置
        FileInputFormat.setInputPaths(job, new Path("/ZHadoop/textHadoop/TopN/input"));

        //使用本地模式指定处理完成后的结果所保持的位置
        FileOutputFormat.setOutputPath(job, new Path("/ZHadoop/textHadoop/TopN/output"));

        //提交程序并且监控打印程序执行情况
        boolean res = job.waitForCompletion(true);
        if (res) {
            FileReader fr = new FileReader("/ZHadoop/textHadoop/TopN/output/part-r-00000");
            BufferedReader reader= new BufferedReader(fr);
            String str;
            while ( (str = reader.readLine()) != null )
                System.out.println(str);

            System.out.println("运行成功");
        }

        System.exit(res ? 0 : 1);
    }
}
