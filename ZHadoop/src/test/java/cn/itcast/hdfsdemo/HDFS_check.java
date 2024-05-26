package cn.itcast.hdfsdemo;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.*;
import org.junit.Before;
import org.junit.Test;

import java.io.IOException;

public class HDFS_check {
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
    //查看目录信息，只显示文件
    @Test
    public void testListFiles() throws IOException {
        //获取迭代器对象
        //RemoteIterator<LocatedFileStatus> listFiles = fs.listFiles(new Path("/"), true);
        RemoteIterator<LocatedFileStatus> liFiles = fs.listFiles(new Path("/helloword.txt"), true);

        //遍历迭代器
        while (liFiles.hasNext()) {
            LocatedFileStatus fileStatus = liFiles.next();

            //打印当前文件名
            System.out.println(fileStatus.getPath().getName());
            ////打印当前文件块大小
            System.out.println(fileStatus.getBlockSize());
            //打印当前文件的权限
            System.out.println(fileStatus.getPermission());
            //打印当前文件内容的长度
            System.out.println(fileStatus.getLen());
            //获取文件块信息（块长度、块的ｄａｔａｎｏｄｅ信息
            BlockLocation[] blockLocations = fileStatus.getBlockLocations();
            for (BlockLocation bl : blockLocations) {
                System.out.println("blick-length:" + bl.getLength() + "--" + "block-offset:" + bl.getOffset());
                String[] hosts = bl.getHosts();
                for (String host : hosts) {
                    System.out.println(host);
                }
            }
            System.out.println("-------------分割线--------------");
        }
    }
}
