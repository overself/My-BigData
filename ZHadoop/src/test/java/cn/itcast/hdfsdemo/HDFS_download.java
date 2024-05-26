package cn.itcast.hdfsdemo;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.junit.Before;
import org.junit.Test;

import java.io.IOException;

public class HDFS_download {
    FileSystem fs = null;
    @Before
    public void init() throws Exception {
        //构建配置参数对象：Configuration
        Configuration conf = new Configuration();
        //设置参数，指定要访问的文件系统的类型：HDFS文件系统
        conf.set("fs.defaultFS","hdfs://hadoop01.bgd01:9000");
        //设置客户端的访问身份，以root身份访问HDFS
        System.setProperty("HADOOP_USER_NAME","root");
        //通过FileSystem类的静态方法，获取文件系统客户端对象
        fs = FileSystem.get(conf);
    }
    //从HDFS下载文件到本地
    @Test
    public void testDownLoadFileToLocal() throws IOException {
        //下载文件
        fs.copyToLocalFile(new Path("/helloword.txt"), new Path("/ZHadoop/textHadoop/HdfsDemo/output"));
        //关闭资源
        fs.close();
    }
}
