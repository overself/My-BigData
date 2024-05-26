package com.ignite.project.config;

import org.apache.ignite.Ignition;
import org.apache.ignite.client.IgniteClient;
import org.apache.ignite.configuration.ClientConfiguration;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;

import java.util.ArrayList;
import java.util.List;

//@Configuration
public class IgniteConfigThin {

    /**
     * 初始化ignite节点信息
     * @return Ignite
     */
    //@Bean
    public IgniteClient igniteClient(){

        // 配置一个客户端的Configuration
        ClientConfiguration clientConfig = new ClientConfiguration();
        // 设置集群地址和端口，这里假设集群在本地运行且端口为10800
        clientConfig.setAddressesFinder(() -> {
            List<String> addresses = new ArrayList<>();
            for (int port = 10800; port <10802; port++) {
                addresses.add("127.0.0.1:" + port);
            }
            return addresses.toArray(new String[]{});
        });
        clientConfig.setTimeout(3000);
        // 分区感知使得瘦客户端可以将请求直接发给待处理数据所在的节点。
        clientConfig.setPartitionAwarenessEnabled(true);

        // 启动这个节点
        return Ignition.startClient(clientConfig);
    }

}
