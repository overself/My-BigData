package com.ignite.project.config;

import lombok.Data;
import org.springframework.boot.context.properties.ConfigurationProperties;
import org.springframework.context.annotation.Configuration;

@Data
@Configuration
@ConfigurationProperties(prefix = "ignite.config")
public class ConfigProperties {

    private String igniteInstanceName = "IgniteClientNode";

    private String workDirectory = "/tem/ignite";

    private boolean peerClassEnabled = false;

    private String gridCfgPath;

    private ClientConnector clientConnector = new ClientConnector();

    private Discovery discovery = new Discovery();


    @Data
    @Configuration
    @ConfigurationProperties(prefix = "ignite.config.connector")
    public static class ClientConnector {
        private Integer port = 10800;

        private Integer portRange = 4;
    }

    @Data
    @Configuration
    @ConfigurationProperties(prefix = "ignite.config.discovery")
    public static class Discovery {

        private String spiType = "TcpMulticast";

        private String connection = "localhost:47500";

        private Integer socketTimeout = 30000;

        private Integer connectionRecoveryTimeout = 50000;
    }

}
