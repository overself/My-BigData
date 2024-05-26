package com.ignite.project.config;

import com.google.common.collect.Lists;
import lombok.extern.slf4j.Slf4j;
import org.apache.ignite.Ignite;
import org.apache.ignite.Ignition;
import org.apache.ignite.cache.CacheMode;
import org.apache.ignite.cache.spring.SpringCacheManager;
import org.apache.ignite.configuration.CacheConfiguration;
import org.apache.ignite.configuration.ClientConnectorConfiguration;
import org.apache.ignite.configuration.IgniteConfiguration;
import org.apache.ignite.configuration.NearCacheConfiguration;
import org.apache.ignite.events.EventType;
import org.apache.ignite.spi.discovery.tcp.TcpDiscoverySpi;
import org.apache.ignite.spi.discovery.tcp.ipfinder.multicast.TcpDiscoveryMulticastIpFinder;
import org.apache.ignite.spi.discovery.zk.ZookeeperDiscoverySpi;
import org.apache.ignite.transactions.spring.SpringTransactionManager;
import org.springframework.cache.annotation.EnableCaching;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.transaction.TransactionManager;

import javax.annotation.Resource;

@Slf4j
@Configuration
@EnableCaching
public class IgniteConfig {

    @Resource
    private ConfigProperties cfgProperties;

    /**
     * IgniteConfiguration配置
     *
     * @return
     */
    @Bean
    public IgniteConfiguration igniteConfiguration() {
        // 配置一个节点的Configuration
        IgniteConfiguration igniteConfig = new IgniteConfiguration();
        //The node will be started as a client node.
        igniteConfig.setClientMode(true);
        // 设置该节点名称
        igniteConfig.setIgniteInstanceName(cfgProperties.getIgniteInstanceName());
        igniteConfig.setWorkDirectory(cfgProperties.getWorkDirectory());

        // Classes of custom Java logic will be transferred over the wire from this app.
        igniteConfig.setPeerClassLoadingEnabled(cfgProperties.isPeerClassEnabled());

        ////Enable task execution events setting
        //igniteConfig.setIncludeEventTypes(
        //        EventType.EVT_TASK_STARTED, EventType.EVT_TASK_FINISHED, EventType.EVT_TASK_FAILED,
        //        EventType.EVT_TASK_TIMEDOUT, EventType.EVT_TASK_SESSION_ATTR_SET, EventType.EVT_TASK_REDUCED,
        //        EventType.EVT_CACHE_OBJECT_PUT, EventType.EVT_CACHE_OBJECT_READ, EventType.EVT_CACHE_OBJECT_REMOVED);

        ConfigProperties.ClientConnector clientConnector = cfgProperties.getClientConnector();
        // 配置一个客户端的连接器
        ClientConnectorConfiguration clientConnectorCfg = new ClientConnectorConfiguration();
        //Set a port range from 10800 to 10803
        clientConnectorCfg.setPort(clientConnector.getPort());
        clientConnectorCfg.setPortRange(clientConnector.getPortRange());
        //允许瘦客户端连接
        //clientConnectorCfg.setThinClientEnabled(true);
        //igniteConfig.setClientConnectorConfiguration(clientConnectorCfg);

        // Setting up an IP Finder to ensure the client can locate the servers.
        ConfigProperties.Discovery discovery = cfgProperties.getDiscovery();
        if ("zookeeper".equalsIgnoreCase(discovery.getSpiType())) {
            ZookeeperDiscoverySpi zkDiscoverySpi = new ZookeeperDiscoverySpi();
            zkDiscoverySpi.setZkConnectionString(discovery.getConnection());
            zkDiscoverySpi.setSessionTimeout(discovery.getSocketTimeout());
            zkDiscoverySpi.setZkRootPath("/apacheIgnite");
            zkDiscoverySpi.setJoinTimeout(discovery.getConnectionRecoveryTimeout());
            igniteConfig.setDiscoverySpi(zkDiscoverySpi);
        } else {
            TcpDiscoveryMulticastIpFinder ipFinder = new TcpDiscoveryMulticastIpFinder();
            ipFinder.setAddresses(Lists.newArrayList(discovery.getConnection().split(",")));
            TcpDiscoverySpi tcpDiscoverySpi = new TcpDiscoverySpi().setIpFinder(ipFinder);
            tcpDiscoverySpi.setSocketTimeout(discovery.getSocketTimeout());
            tcpDiscoverySpi.setConnectionRecoveryTimeout(discovery.getConnectionRecoveryTimeout());
            igniteConfig.setDiscoverySpi(tcpDiscoverySpi);
        }
        igniteConfig.setLifecycleBeans(new IgniteLifecycleBeanStart(), new IgniteLifecycleBeanStop());
        return igniteConfig;
    }

    /**
     * 初始化ignite节点信息
     * @param igniteConfig
     * @return Ignite
     */
    @Bean
    public Ignite igniteInstance(IgniteConfiguration igniteConfig) {
        // 启动这个节点
        Ignite ignite = Ignition.start(igniteConfig);
        log.info("Ignite is started!");
        return ignite;
    }

    /**
     * Ignite胖客户端缓存管理器配置
     *
     * @param ignite
     * @return
     */
    @Bean
    public SpringCacheManager cacheManager(Ignite ignite) {
        SpringCacheManager mgr = new SpringCacheManager();
        mgr.setIgniteInstanceName(ignite.name());

        // Other required configuration parameters.
        CacheConfiguration configuration = new CacheConfiguration<>();
        configuration.setCacheMode(CacheMode.REPLICATED);
        mgr.setDynamicCacheConfiguration(configuration);


        NearCacheConfiguration nearCacheConfig = new NearCacheConfiguration<>();
        nearCacheConfig.setNearStartSize(1000);
        mgr.setDynamicNearCacheConfiguration(nearCacheConfig);
        return mgr;
    }

    @Bean
    public TransactionManager transactionManager(Ignite ignite) {
        SpringTransactionManager mgr = new SpringTransactionManager();
        mgr.setIgniteInstanceName(ignite.name());
        return mgr;
    }
}
