package com.ignite.project.demo.binary;

import lombok.extern.slf4j.Slf4j;
import org.apache.ignite.Ignite;
import org.apache.ignite.IgniteCache;
import org.apache.ignite.binary.BinaryField;
import org.apache.ignite.binary.BinaryObject;
import org.apache.ignite.binary.BinaryObjectBuilder;
import org.apache.ignite.binary.BinaryType;
import org.apache.ignite.configuration.CacheConfiguration;
import org.springframework.stereotype.Component;

import javax.annotation.Resource;
import javax.cache.processor.EntryProcessor;
import javax.cache.processor.EntryProcessorException;
import javax.cache.processor.MutableEntry;

/**
 * 二进制对象
 * BinaryObject格式实现隐含了一些限制：
 * 如果对象可以被序列化到二进制形式，那么Ignite会在序列化期间计算它的哈希值并且将其写入最终的二进制数组。
 * 另外，Ignite还为二进制对象的比较提供了equals方法的自定义实现。
 * 这意味着不需要为在Ignite中使用自定义键和值覆写GetHashCode和Equals方法，除非它们无法序列化成二进制形式。
 */
@Slf4j
@Component
public class BinaryObjectExample {

    @Resource
    private Ignite ignite;

    private final String OBJECT_STORE_CACHE_NAME = this.getClass().getSimpleName();


    public void doProcessBinaryObject() {

        CacheConfiguration<Integer, BinaryPojo> pojoCfg = new CacheConfiguration<Integer, BinaryPojo>(OBJECT_STORE_CACHE_NAME);
        BinaryPojo pojo = new BinaryPojo(1, "My Process Binary Object", "BinaryObject API");
        //默认true，使用BinaryObject，当该标志设置为false
        //pojoCfg.setStoreKeepBinary(false);
        //TODO：CacheStore待调查
        //pojoCfg.setCacheStoreFactory((Factory<? extends CacheStore<? super Integer,? super BinaryPojo>>) FactoryBuilder.factoryOf(CacheExampleBinaryStore.class));

        try (IgniteCache<Integer, BinaryPojo> pojoCache = ignite.getOrCreateCache(pojoCfg)) {
            pojoCache.put(pojo.getId(), pojo);
        }

        try (IgniteCache<Integer, BinaryObject> pojoCache = ignite.getOrCreateCache(pojoCfg).withKeepBinary()) {
            BinaryObject result = pojoCache.get(pojo.getId());
            log.info("Process Binary Object: {}", result);
            log.info("Process Binary Object->pojo: {}", (BinaryPojo) result.deserialize());

            BinaryType resultType = result.type();
            log.info("resultType: {}->{}", resultType.typeId(), resultType);
            BinaryField field = resultType.field("context");
            log.info("BinaryField: {} => {}", field, field.value(result));

            //BinaryObject实例是不可变的，要更新属性或者创建新的BinaryObject，必须使用BinaryObjectBuilder的实例。
            //下面用BinaryObjectAPI来处理服务端节点的数据而不需要将程序部署到服务端以及不需要实际的数据反序列化的
            BinaryObject invokePoJo = pojoCache.invoke(pojo.getId(), new EntryProcessor<Integer, BinaryObject, BinaryObject>() {
                @Override
                public BinaryObject process(MutableEntry<Integer, BinaryObject> mutableEntry, Object... arguments) throws EntryProcessorException {
                    BinaryObjectBuilder builder = mutableEntry.getValue().toBuilder();
                    builder.setField("context", "Ignite->" + (String) builder.getField("context"));
                    builder.setField("name", "Ignite->" + (String) builder.getField("name"), String.class);
                    mutableEntry.setValue(builder.build());
                    Object value = mutableEntry.getValue();
                    log.info("BinaryObjectBuilder==={}", value);
                    return mutableEntry.getValue();
                }
            });
            log.info("EntryProcessor invokePoJo: {}", (BinaryPojo) invokePoJo.deserialize());
        }

        try (IgniteCache<Integer, BinaryPojo> pojoCache = ignite.getOrCreateCache(pojoCfg)) {
            BinaryPojo result = pojoCache.get(pojo.getId());
            log.info("Process invoked Pojo Object: {}", result);
        } finally {
            ignite.destroyCache(pojoCfg.getName());
        }
    }

}
