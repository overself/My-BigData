package com.spark.project.config.hadoop;

import lombok.extern.slf4j.Slf4j;
import org.apache.hadoop.fs.FileSystem;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;

import javax.annotation.Resource;
import java.net.URI;

@Slf4j
@Configuration
public class HadoopFSConfiguration {

    @Resource
    private HadoopProperties properties;

    @Bean
    public org.apache.hadoop.conf.Configuration configuration() {
        log.info("hadoop server init");
        org.apache.hadoop.conf.Configuration conf = new org.apache.hadoop.conf.Configuration();
        conf.addResource("hadoop/core-site.xml");
        conf.addResource("hadoop/yarn-site.xml");
        conf.addResource("hadoop/hdfs-site.xml");
        conf.addResource("hadoop/mapred-site.xml");
        conf.reloadConfiguration();
        //conf.set("fs.defaultFS", properties.getHdfs().getUri());
        //conf.set("dfs.replication", "2");
        //conf.set("yarn.resourcemanager.hostname", properties.getYarn().getHostName());
        conf.set("mapreduce.framework.name", "yarn");
        //conf.set("mapreduce.framework.name", "local");
        conf.set("fs.AbstractFileSystem.hdfs.impl", "org.apache.hadoop.fs.Hdfs");
        conf.set("mapreduce.app-submission.cross-platform", "true");
        return conf;
    }

    @Bean
    public FileSystem getFileSystem(org.apache.hadoop.conf.Configuration config) {
        HadoopProperties.Hdfs hdfs = properties.getHdfs();
        FileSystem fileSystem = null;
        try {
            fileSystem = FileSystem.newInstance(URI.create(hdfs.getUri()), config, hdfs.getUser());
        } catch (Exception e) {
            log.error(e.getMessage());
        }
        return fileSystem;
    }
}
