package com.ignite.project.config;

import lombok.extern.slf4j.Slf4j;
import org.apache.ignite.Ignite;
import org.apache.ignite.IgniteException;
import org.apache.ignite.cluster.ClusterState;
import org.apache.ignite.lifecycle.LifecycleBean;
import org.apache.ignite.lifecycle.LifecycleEventType;
import org.apache.ignite.resources.IgniteInstanceResource;

@Slf4j
public class IgniteLifecycleBeanStart implements LifecycleBean {
    @IgniteInstanceResource
    public Ignite ignite;

    @Override
    public void onLifecycleEvent(LifecycleEventType evt) throws IgniteException {
        if (evt == LifecycleEventType.AFTER_NODE_START) {
            ignite.cluster().state(ClusterState.ACTIVE);
            log.info("After the node (consistentId = {}) starts.", ignite.cluster().node().consistentId());
        }
    }
}
