package com.ignite.project.demo;

import com.ignite.project.BootBaseTest;
import com.ignite.project.demo.affinity.AffinityCollocationExample;
import com.ignite.project.demo.binary.BinaryObjectExample;
import com.ignite.project.demo.dim.ObjectCacheExample;
import lombok.extern.slf4j.Slf4j;
import org.apache.ignite.Ignite;
import org.apache.ignite.IgniteCache;
import org.apache.ignite.IgniteException;
import org.apache.ignite.cache.CacheMode;
import org.apache.ignite.configuration.CacheConfiguration;
import org.junit.jupiter.api.MethodOrderer;
import org.junit.jupiter.api.Order;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.TestMethodOrder;

import javax.annotation.Resource;

@Slf4j
@TestMethodOrder(value = MethodOrderer.OrderAnnotation.class)
public class StartIgniteInstanceTest extends BootBaseTest {

    @Resource
    private Ignite ignite;

    @Resource
    private ObjectCacheExample objectCache;

    @Resource
    private AffinityCollocationExample affinityCache;

    @Resource
    private BinaryObjectExample binaryObjectExample;

    @Test
    @Order(1)
    public void igniteVersionTest() {
        log.info("ignite version: {}, Instance name: {}", ignite.version(), ignite.configuration().getIgniteInstanceName());
    }

    @Test
    @Order(2)
    public void objectCacheTest() {
        objectCache.saveDimStore();
    }

    @Test
    @Order(3)
    public void affinityCacheTest() {
        affinityCache.configureAffinityKeyWithAnnotation();
        affinityCache.configureAffinityKeyWithCacheKeyConfiguration();
        affinityCache.configureAffinityKeyWithAffinityKeyClass();
    }

    @Test
    @Order(4)
    public void processBinaryObjectTest() {
        binaryObjectExample.doProcessBinaryObject();
    }

}
