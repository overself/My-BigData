package cn.itcast.hdfsdemo;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.junit.Before;
import org.junit.Test;

import java.io.IOException;

public class HDFS_operate {
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
    //在HDFS上创建、删除、重命名文件
    @Test
    public void testMkdirAndDeleteAndRename() throws IOException {
/*        //创建目录
        fs.mkdirs(new Path("/a/b/c"));
        fs.mkdirs(new Path("/a2/b2/c2"));
        //重命名文件或文件夹
        fs.rename(new Path("/a"), new Path("/a3"));*/
        //删除文件夹，如果是非空文件夹。参数2必须给值true
        fs.delete(new Path("/a2"), true);
        //关闭资源
        fs.close();
    }
}
