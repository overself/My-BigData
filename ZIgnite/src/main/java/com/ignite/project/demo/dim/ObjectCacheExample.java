package com.ignite.project.demo.dim;

import lombok.extern.slf4j.Slf4j;
import org.apache.ignite.Ignite;
import org.apache.ignite.IgniteCache;
import org.apache.ignite.binary.BinaryObject;
import org.apache.ignite.cache.CacheMode;
import org.apache.ignite.cache.query.*;
import org.apache.ignite.configuration.CacheConfiguration;
import org.apache.ignite.internal.binary.BinaryContext;
import org.apache.ignite.internal.binary.GridBinaryMarshaller;
import org.springframework.stereotype.Component;

import javax.annotation.Resource;
import javax.cache.Cache;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;

import static org.apache.ignite.cache.query.IndexQueryCriteriaBuilder.eq;

@Slf4j
@Component
public class ObjectCacheExample {

    @Resource
    private Ignite ignite;

    private final String DIM_STORE_CACHE_NAME = ObjectCacheExample.class.getSimpleName();

    public void saveDimStore() {

        try {
            //BinaryContext context = GridBinaryMarshaller.threadLocalContext();
            //context.registerPredefinedType(DimStore.class, -1);

            // Auto-close cache at the end of the example.
            DimStore store1 = new DimStore(1, "Store1", "12345", "32221 Chilly Dr, NY");
            DimStore store2 = new DimStore(2, "Store2", "54321", "122223 Windy Dr, San Francisco");
            try (IgniteCache<Integer, DimStore> dimStoreCache = ignite.getOrCreateCache(getDimStoreConfig())) {
                dimStoreCache.put(store1.getId(), store1);
                dimStoreCache.put(store2.getId(), store2);
                for (int index = 3; index < 1000; index++) {
                    DimStore store = new DimStore(index, "Store1" + index, "12345" + index, "32221 Chilly Dr, NY");
                    dimStoreCache.put(store1.getId(), store);
                }
            }

            try (IgniteCache<Integer, DimStore> dimStoreCache = ignite.getOrCreateCache(getDimStoreConfig())) {
                DimStore dimStore = dimStoreCache.get(2);
                log.info("Get dimStore By Key={}", dimStore);
            }

            try (IgniteCache<Integer, DimStore> dimStoreCache = ignite.getOrCreateCache(getDimStoreConfig())) {
                String sql = "select * from \"" + DIM_STORE_CACHE_NAME + "\".DimStore where zip=? and addr like ? ";
                SqlQuery<Integer, DimStore> sqlQuery = new SqlQuery<>(DimStore.class, sql);
                sqlQuery.setArgs("12345", "%222%");
                QueryCursor<Cache.Entry<Integer, DimStore>> queryCache = dimStoreCache.query(sqlQuery);
                List<Cache.Entry<Integer, DimStore>> result = queryCache.getAll();
                result.forEach(item -> log.info("Get dimStore By SqlQuery={}", item.getValue()));
            }

            try (IgniteCache<Integer, DimStore> dimStoreCache = ignite.getOrCreateCache(getDimStoreConfig())) {
                QueryCursor<List<?>> storePurchases = dimStoreCache.query(new SqlFieldsQuery(
                        "select fp.* from DimStore fp "
                                + "where name=?").setArgs("Store1").setSchema(DIM_STORE_CACHE_NAME));
                Iterable<List<?>> stores = storePurchases.getAll();
                for (List<?> items : stores) {
                    items.forEach(item -> log.info("SqlFieldsQuery Data Row item={}", item));
                }
            }

            try (IgniteCache<Integer, DimStore> dimStoreCache = ignite.getOrCreateCache(getDimStoreConfig())) {
                IndexQuery<Integer, BinaryObject> indexQuery = new IndexQuery<>(DimStore.class, "DIM_STORE_IDX");
                indexQuery.setCriteria(eq("name", "Store1"));
                Map<String, String> cond = new HashMap<>();
                cond.put("addr", "222");
                indexQuery.setFilter(new DimStorePredicate(cond));
                List<Cache.Entry<Integer, BinaryObject>> result = dimStoreCache.withKeepBinary().query(indexQuery).getAll();
                for (Cache.Entry<Integer, BinaryObject> items : result) {
                    log.info("IndexQuery BinaryObject.key={}", items.getKey());
                    log.info("IndexQuery BinaryObject.value={}", Optional.ofNullable(items.getValue().deserialize()).get());
                }
            }

            try (IgniteCache<Integer, BinaryObject> dimStoreCache = ignite.getOrCreateCache(getDimStoreConfig()).withKeepBinary()) {
                Map<String, String> cond = new HashMap<>();
                cond.put("addr", "222");
                //扫描查询支持可选的转换器闭包，支持Remote filte，可以在数据返回之前在服务端转换数据
                ScanQuery<Integer, BinaryObject> scan = new ScanQuery<>(new DimStorePredicate(cond));
                QueryCursor<Cache.Entry<Integer, BinaryObject>> entries = dimStoreCache.query(scan);
                List<Cache.Entry<Integer, BinaryObject>> stores = entries.getAll();
                for (Cache.Entry<Integer, BinaryObject> items : stores) {
                    log.info("ScanQuery BinaryObject.key={}", items.getKey());
                    log.info("ScanQuery BinaryObject.value={}", Optional.ofNullable((DimStore) items.getValue().deserialize()).get());
                }
            }
        } finally {
            ignite.destroyCache(DIM_STORE_CACHE_NAME);
        }
    }

    private CacheConfiguration<Integer, DimStore> getDimStoreConfig() {
        CacheConfiguration<Integer, DimStore> dimStoreCacheCfg = new CacheConfiguration<>(DIM_STORE_CACHE_NAME);
        dimStoreCacheCfg.setCacheMode(CacheMode.REPLICATED);
        dimStoreCacheCfg.setIndexedTypes(Integer.class, DimStore.class);
        dimStoreCacheCfg.setStatisticsEnabled(true);
        dimStoreCacheCfg.setManagementEnabled(true);
        return dimStoreCacheCfg;
    }


}
