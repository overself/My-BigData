package com.spark.project.config.spark;

import lombok.Data;
import org.springframework.boot.context.properties.ConfigurationProperties;
import org.springframework.context.annotation.Configuration;

import java.util.HashMap;

@Data
@Configuration
@ConfigurationProperties(prefix = "spring.spark")
public class SparkProperties {

    private final static String DEFAULT_APP_NAME = "spark-app";

    private final static int CPU_CORE = Runtime.getRuntime().availableProcessors();

    private final static String DEFAULT_MASTER = "local[" + CPU_CORE + "]";

    private HashMap<String, String> other = new HashMap<>();

    private String hadoopHome = "/usr/local/hadoop";
    private String appNamePrefix = DEFAULT_APP_NAME;

    private String master = DEFAULT_MASTER;

    private SparkDriver driver;

    @Data
    public static class SparkDriver {
        private String host;

        private String port;
    }
}
