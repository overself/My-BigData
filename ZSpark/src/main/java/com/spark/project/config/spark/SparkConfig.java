package com.spark.project.config.spark;

import cn.hutool.core.util.StrUtil;
import org.apache.spark.SparkConf;
import org.apache.spark.SparkContext;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.sql.SparkSession;
import org.springframework.boot.autoconfigure.condition.ConditionalOnMissingBean;
import org.springframework.boot.context.properties.EnableConfigurationProperties;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;

import java.io.IOException;
import java.net.InetAddress;
import java.net.ServerSocket;
import java.net.UnknownHostException;

@Configuration
@EnableConfigurationProperties({SparkProperties.class})
public class SparkConfig {

    @Bean
    public SparkConf sparkConf(SparkProperties sparkProperties) {
        InetAddress inetAddress;
        try {
            inetAddress = InetAddress.getLocalHost();
        } catch (UnknownHostException e) {
            throw new RuntimeException(e);
        }
        SparkConf conf = new SparkConf();
        conf.setAppName(sparkProperties.getAppNamePrefix());
        conf.setMaster(sparkProperties.getMaster());
        //使用conf::set设置conf，取代java容器到scala容器的转化
        sparkProperties.getOther().forEach(conf::set);
        SparkProperties.SparkDriver driver = sparkProperties.getDriver();
        if (driver != null) {
            conf.set("spark.driver.host", StrUtil.isNotBlank(driver.getHost()) ? driver.getHost() : inetAddress.getHostAddress());
            conf.set("spark.driver.port", driver.getPort());
        } else {
            conf.set("spark.driver.host", inetAddress.getHostAddress());
            conf.set("spark.driver.port", String.valueOf(getHostFreePort()));
        }
        return conf;
    }

    @Bean
    @ConditionalOnMissingBean(SparkSession.class)
    public SparkSession sparkSession(SparkConf conf) {
        SparkSession session = SparkSession.builder().config(conf).getOrCreate();
        return session;
    }

    @Bean
    @ConditionalOnMissingBean(JavaSparkContext.class)
    public JavaSparkContext javaSparkContext(SparkConf conf) {
        return new JavaSparkContext(SparkContext.getOrCreate(conf));
    }

    public int getHostFreePort() {
        try {
            // 创建一个ServerSocket，传入0作为端口号表示让系统自动分配一个空闲端口
            ServerSocket serverSocket = new ServerSocket(0);
            int port = serverSocket.getLocalPort();
            serverSocket.close();
            return port;
        } catch (IOException e) {
            return -1;
        }
    }
}
