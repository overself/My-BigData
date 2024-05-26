package com.ignite.project.config;

import lombok.extern.slf4j.Slf4j;
import org.apache.ignite.Ignite;
import org.apache.ignite.IgniteException;
import org.apache.ignite.cluster.ClusterState;
import org.apache.ignite.lifecycle.LifecycleBean;
import org.apache.ignite.lifecycle.LifecycleEventType;
import org.apache.ignite.resources.IgniteInstanceResource;

@Slf4j
public class IgniteLifecycleBeanStop implements LifecycleBean {
    @IgniteInstanceResource
    public Ignite ignite;

    @Override
    public void onLifecycleEvent(LifecycleEventType evt) throws IgniteException {
        if (evt == LifecycleEventType.BEFORE_NODE_STOP) {
            log.info("before the node (consistentId = {}) stops.", ignite.cluster().node().consistentId());
            ignite.cluster().state(ClusterState.ACTIVE);
        }
    }
}
