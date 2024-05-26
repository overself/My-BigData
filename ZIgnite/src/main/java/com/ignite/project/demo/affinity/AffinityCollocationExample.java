package com.ignite.project.demo.affinity;

import com.ignite.project.common.SnowFlakeUtil;
import lombok.extern.slf4j.Slf4j;
import org.apache.ignite.Ignite;
import org.apache.ignite.IgniteCache;
import org.apache.ignite.cache.CacheKeyConfiguration;
import org.apache.ignite.cache.CacheWriteSynchronizationMode;
import org.apache.ignite.cache.affinity.AffinityKey;
import org.apache.ignite.configuration.CacheConfiguration;
import org.springframework.stereotype.Component;

import javax.annotation.Resource;

/**
 * 关联并置: 如果不同的条目经常一起访问，则将它们并置在一起就很有用, 在一个节点（存储对象的节点）上就可以执行多条目查询，这个概念称为关联并置.
 * 关联函数将条目分配给分区，具有相同关联键的对象将进入相同的分区
 * 如果要通过不同的字段并置来自两个缓存的数据，则必须使用复杂的对象作为键。该对象通常包含一个唯一地标识该缓存中的对象的字段，以及一个要用于并置的字段。
 */
@Slf4j
@Component
public class AffinityCollocationExample {

    @Resource
    private Ignite ignite;

    private final String PERSON_STORE_CACHE_NAME = AffinityCollocationExample.class.getSimpleName() + "Persons";

    private final String COMPANY_STORE_CACHE_NAME = AffinityCollocationExample.class.getSimpleName() + "Companies";

    private final String ROLE_STORE_CACHE_NAME = AffinityCollocationExample.class.getSimpleName() + "Roles";

    public void configureAffinityKeyWithAnnotation() {

        try {
            CacheConfiguration<PersonKey, Person> personCfg = new CacheConfiguration<>(PERSON_STORE_CACHE_NAME);
            personCfg.setBackups(1);

            CacheConfiguration<String, Company> companyCfg = new CacheConfiguration<>(COMPANY_STORE_CACHE_NAME);
            companyCfg.setBackups(1);

            IgniteCache<PersonKey, Person> personCache = ignite.getOrCreateCache(personCfg);
            IgniteCache<String, Company> companyCache = ignite.getOrCreateCache(companyCfg);

            Company company1 = new Company(SnowFlakeUtil.getSnowFlakeId(), "My company");
            Person person1 = new Person(SnowFlakeUtil.getSnowFlakeId(), "John", company1.getId(), null, null);

            // Both the p1 and c1 objects will be cached on the same node
            personCache.put(new PersonKey(person1.getId(), company1.getId(), null), person1);
            companyCache.put("company1", company1);

            // Get the person object
            person1 = personCache.get(new PersonKey(person1.getId(), company1.getId(), null));
            log.info("Annotation Person {}", person1);
        } finally {
            ignite.destroyCache(PERSON_STORE_CACHE_NAME);
            ignite.destroyCache(COMPANY_STORE_CACHE_NAME);
        }
    }

    public void configureAffinityKeyWithCacheKeyConfiguration() {
        CacheConfiguration<PersonKey, Person> personCfg = new CacheConfiguration<>(PERSON_STORE_CACHE_NAME);
        personCfg.setBackups(1);
        //使用CacheKeyConfiguration类在缓存配置中配置关联键
        personCfg.setKeyConfiguration(new CacheKeyConfiguration("Person", "roleId"));

        CacheConfiguration<Long, Role> roleCfg = new CacheConfiguration<>(ROLE_STORE_CACHE_NAME);
        roleCfg.setBackups(1);

        CacheConfiguration<Long, Company> companyCfg = new CacheConfiguration<>(COMPANY_STORE_CACHE_NAME);
        companyCfg.setBackups(1);

        try (IgniteCache<PersonKey, Person> personCache = ignite.getOrCreateCache(personCfg);
             IgniteCache<Long, Role> roleCache = ignite.getOrCreateCache(roleCfg);
             IgniteCache<Long, Company> companyCache = ignite.getOrCreateCache(companyCfg)) {
            Company company1 = new Company(SnowFlakeUtil.getSnowFlakeId(), "My company");
            Role role1 = new Role(SnowFlakeUtil.getSnowFlakeId(), "Admin");
            Person person1 = new Person(SnowFlakeUtil.getSnowFlakeId(), "John2", company1.getId(), role1.getId(), null);

            // Both the p1 and c1 objects will be cached on the same node
            personCache.put(new PersonKey(person1.getId(), company1.getId(), role1.getId()), person1);
            companyCache.put(company1.getId(), company1);
            roleCache.put(role1.getId(), role1);

            // Get the person object
            person1 = personCache.get(new PersonKey(person1.getId(), company1.getId(), role1.getId()));
            log.info("CacheKeyConfiguration Person {}", person1);
        } finally {
            ignite.destroyCache(personCfg.getName());
            ignite.destroyCache(companyCfg.getName());
        }
    }

    public void configureAffinityKeyWithAffinityKeyClass() {
        CacheConfiguration<AffinityKey<Long>, Person> personCfg = new CacheConfiguration<>(PERSON_STORE_CACHE_NAME);
        personCfg.setBackups(1);
        //PRIMARY_SYNC:默认模式，客户端节点会等待主节点的写入或者提交完成，但是不会等待备份的更新。
        personCfg.setWriteSynchronizationMode(CacheWriteSynchronizationMode.FULL_SYNC);
        //使用AffinityKey类，自定义关联映射
        personCfg.setKeyConfiguration(new CacheKeyConfiguration("Person", "roleId"));

        CacheConfiguration<Long, Role> roleCfg = new CacheConfiguration<>(ROLE_STORE_CACHE_NAME);
        roleCfg.setBackups(1);

        try (IgniteCache<AffinityKey<Long>, Person> personCache = ignite.getOrCreateCache(personCfg);
             IgniteCache<Long, Role> roleCache = ignite.getOrCreateCache(roleCfg)) {

            Company company1 = new Company(SnowFlakeUtil.getSnowFlakeId(), "My company");
            Role role1 = new Role(SnowFlakeUtil.getSnowFlakeId(), "Admin");
            Person person1 = new Person(SnowFlakeUtil.getSnowFlakeId(), "John2", company1.getId(), role1.getId(), null);

            // Both the p1 and c1 objects will be cached on the same node
            personCache.put(new AffinityKey<>(person1.getId(), role1.getId()), person1);
            roleCache.put(role1.getId(), role1);

            // Get the person object
            person1 = personCache.get(new AffinityKey<>(person1.getId(), role1.getId()));
            log.info("AffinityKeyClass Person {}", person1);
        } finally {
            ignite.destroyCache(personCfg.getName());
            ignite.destroyCache(roleCfg.getName());
        }
    }
}
