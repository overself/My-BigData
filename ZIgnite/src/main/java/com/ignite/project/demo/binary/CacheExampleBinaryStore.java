package com.ignite.project.demo.binary;

import org.apache.ignite.Ignite;
import org.apache.ignite.IgniteBinary;
import org.apache.ignite.IgniteCache;
import org.apache.ignite.binary.BinaryObjectBuilder;
import org.apache.ignite.cache.store.CacheStoreAdapter;
import org.apache.ignite.resources.IgniteInstanceResource;

import javax.cache.Cache;
import javax.cache.integration.CacheLoaderException;
import javax.cache.integration.CacheWriterException;

public class CacheExampleBinaryStore<K, V> extends CacheStoreAdapter<K, V> {
    @IgniteInstanceResource
    private Ignite ignite;

    public CacheExampleBinaryStore() {

    }

    @Override
    public V load(K key) throws CacheLoaderException {
        IgniteBinary binary = ignite.binary();
        IgniteCache<K, V> orCreateCache = ignite.getOrCreateCache("");
        V result = orCreateCache.get(key);
        BinaryObjectBuilder bldr = binary.builder("BinaryPojo");
        return result;
    }

    @Override
    public void write(Cache.Entry<? extends K, ? extends V> entry) throws CacheWriterException {

    }

    @Override
    public void delete(Object key) throws CacheWriterException {

    }
}
