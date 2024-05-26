package com.spark.project.config.hadoop;

import lombok.Data;
import org.springframework.boot.context.properties.ConfigurationProperties;
import org.springframework.context.annotation.Configuration;
import org.springframework.stereotype.Component;

import java.util.HashMap;

@Data
@Configuration
@ConfigurationProperties(prefix = "spring.hadoop")
public class HadoopProperties {

    private String hadoopHome = "/usr/local/hadoop";

    private String hadoopUser = "root";

    private Hdfs hdfs;

    private Yarn yarn;

    @Data
    public static class Hdfs{

        private String uri;

        private String user;
    }

    @Data
    public static class Yarn{

        private String hostName;

    }

}
