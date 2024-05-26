package cn.itcast.hdfsdemo;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.junit.Before;
import org.junit.Test;

import java.io.IOException;

public class HDFS_uploading {
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
    //将本地文件上传到HDFS
    @Test
    public void testAddFileToHdfs() throws IOException {
        //要上传的文件所在本地路径
        Path src = new Path("/ZHadoop/textHadoop/HdfsDemo/input/text");
        //要上传到HDFS的目标路径
        Path dst = new Path("/");
        //上传文件
        fs.copyFromLocalFile(src,dst);
        //关闭资源
        fs.close();
    }
}
