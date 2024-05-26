package com.ignite.project.demo.cache;

import com.google.common.collect.Lists;
import com.ignite.project.common.SnowFlakeUtil;
import com.ignite.project.demo.affinity.Company;
import com.ignite.project.demo.affinity.Person;
import lombok.extern.slf4j.Slf4j;
import org.apache.ignite.Ignite;
import org.apache.ignite.IgniteCache;
import org.apache.ignite.binary.BinaryObject;
import org.apache.ignite.binary.BinaryObjectBuilder;
import org.apache.ignite.cache.CacheAtomicityMode;
import org.apache.ignite.cache.CacheMode;
import org.apache.ignite.cache.query.QueryCursor;
import org.apache.ignite.cache.query.SqlQuery;
import org.apache.ignite.configuration.CacheConfiguration;
import org.springframework.cache.annotation.CacheEvict;
import org.springframework.cache.annotation.CachePut;
import org.springframework.cache.annotation.Cacheable;
import org.springframework.stereotype.Component;

import javax.annotation.Nonnull;
import javax.annotation.PostConstruct;
import javax.annotation.Resource;
import javax.cache.Cache;
import javax.cache.processor.EntryProcessor;
import javax.cache.processor.EntryProcessorException;
import javax.cache.processor.MutableEntry;
import java.util.List;

/**
 * cacheable中的condition和unless
 */
@Slf4j
@Component
public class CompanyPersonDao {

    public final static String STORE_CACHE_NAME = "COMPANY_PERSON";

    private CacheConfiguration configurationPerson;

    private CacheConfiguration configurationCompany;
    @Resource
    private Ignite ignite;

    public Long saveCompany(Company company) {
        if (company.getId() == null) {
            company.setId(SnowFlakeUtil.getSnowFlakeId());
        }
        try (IgniteCache<Long, Company> companyCache = ignite.getOrCreateCache(this.configurationCompany)) {
            companyCache.put(company.getId(), company);
        }
        return company.getId();
    }

    public Long savePerson(Person person) {
        if (person.getId() == null) {
            person.setId(SnowFlakeUtil.getSnowFlakeId());
        }
//        try (IgniteCache<AffinityKey<Long>, Person> companyCache = ignite.getOrCreateCache(this.configuration)) {
//            AffinityKey<Long> affinityKey = new AffinityKey<>(person.getId(), person.getCompanyId());
//            companyCache.put(affinityKey, person);
//        }
        try (IgniteCache<Long, Person> companyCache = ignite.getOrCreateCache(this.configurationPerson)) {
            companyCache.put(person.getId(), person);
        }
        return person.getId();
    }

    @CachePut(value = "CacheCompany", key = "#company.id", unless = "#result == null")
    public Company updateCompany(Company company) {
        if (company.getId() == null) {
            throw new RuntimeException("Company Id is null");
        }
        try (IgniteCache<Long, BinaryObject> companyCache = ignite.getOrCreateCache(this.configurationCompany).withKeepBinary()) {
            BinaryObject invokePoJo = companyCache.invoke(company.getId(),
                    new EntryProcessor<Long, BinaryObject, BinaryObject>() {
                        @Override
                        public BinaryObject process(MutableEntry<Long, BinaryObject> mutableEntry, Object... arguments) throws EntryProcessorException {
                            BinaryObjectBuilder builder = mutableEntry.getValue().toBuilder();
                            builder.setField("name", company.getName());
                            mutableEntry.setValue(builder.build());
                            return mutableEntry.getValue();
                        }
                    });
            return invokePoJo.deserialize();
        }
    }

    @CachePut(value = "CachePerson", key = "#person.id", unless = "#result == null")
    public Person updatePerson(Person person) {
        if (person.getId() == null) {
            throw new RuntimeException("Person Id is null");
        }
        try (IgniteCache<Long, BinaryObject> companyCache = ignite.getOrCreateCache(this.configurationPerson).withKeepBinary()) {
            BinaryObject invokePoJo = companyCache.invoke(person.getId(),
                    new EntryProcessor<Long, BinaryObject, BinaryObject>() {
                        @Override
                        public BinaryObject process(MutableEntry<Long, BinaryObject> mutableEntry, Object... arguments) throws EntryProcessorException {
                            BinaryObjectBuilder builder = mutableEntry.getValue().toBuilder();
                            builder.setField("name", person.getName());
                            builder.setField("companyId", person.getCompanyId());
                            builder.setField("roleId", person.getRoleId());
                            mutableEntry.setValue(builder.build());
                            return mutableEntry.getValue();
                        }
                    });
            return invokePoJo.deserialize();
        }
    }

    @CacheEvict(value = "CacheCompany", key = "#companyId")
    public boolean deleteCompany(Long companyId) {
        if (companyId == null) {
            throw new RuntimeException("Company Id is null");
        }
        try (IgniteCache<Long, BinaryObject> companyCache = ignite.getOrCreateCache(this.configurationCompany)) {
            if (!companyCache.remove(companyId)) {
                throw new RuntimeException("Delete Company[" + companyId + "] is fail");
            }
            return true;
        }
    }

    @CacheEvict(value = "CachePerson", key = "#personId")
    public boolean deletePerson(Long personId) {
        if (personId == null) {
            throw new RuntimeException("Person Id is null");
        }
        try (IgniteCache<Long, BinaryObject> companyCache = ignite.getOrCreateCache(this.configurationPerson)) {
            if (!companyCache.remove(personId)) {
                throw new RuntimeException("Delete Person " + personId + " is fail");
            }
            return true;
        }
    }

    @Cacheable(value = "CacheCompany", key = "#companyId", unless = "#result == null")
    public Company getCompanyById(Long companyId) {
        if (companyId == null) {
            throw new RuntimeException("Id is null");
        }
        try (IgniteCache<Long, Company> personCache = ignite.getOrCreateCache(this.configurationCompany)) {
            return personCache.get(companyId);
        }
    }

    @Cacheable(value = "CachePerson", key = "#peronId", unless = "#result == null")
    public Person getPersonById(Long peronId) {
        log.info("查询用户：{}", peronId);
        if (peronId == null) {
            throw new RuntimeException("Id is null");
        }
        try (IgniteCache<Long, Person> personCache = ignite.getOrCreateCache(this.configurationPerson)) {
            return personCache.get(peronId);
        }
    }

    @Cacheable(value = "CachePersonForCompany", key = "#companyId+'_'+#personName", unless = "#result == null")
    public List<Person> getPersonListOfCompany(@Nonnull Long companyId, String personName) {
        log.info("查询某公司的用户：{} , {}", companyId, personName);
        if (companyId == null) {
            throw new RuntimeException("Company Id is null");
        }
        List<Person> personList = Lists.newArrayList();
        try (IgniteCache<Long, Person> personCache = ignite.getOrCreateCache(this.configurationPerson)) {
            String sql = "select * from \"" + this.configurationPerson.getName() + "\".Person where companyId=? ";
            List<Object> args = Lists.newArrayList(companyId);
            if (personName != null) {
                sql = sql + " and name like ? ";
                args.add("%" + personName + "%");
            }
            SqlQuery<Long, Person> sqlQuery = new SqlQuery<>(Person.class, sql);
            sqlQuery.setArgs(args.stream().toArray());
            QueryCursor<Cache.Entry<Long, Person>> queryCache = personCache.query(sqlQuery);
            List<Cache.Entry<Long, Person>> result = queryCache.getAll();
            result.forEach(item -> personList.add(item.getValue()));
        }
        return personList;
    }

    @Cacheable(value = "CacheCompanyList", key = "#name", unless = "#result == null")
    public List<Company> getCompanyList(String name, @Nonnull Long companyId) {
        log.info("查询某公司：{} , {}", companyId, name);
        if (name == null) {
            throw new RuntimeException("Company Id is null");
        }
        List<Company> personList = Lists.newArrayList();
        try (IgniteCache<Long, Company> personCache = ignite.getOrCreateCache(this.configurationCompany)) {
            String sql = "select * from \"" + this.configurationCompany.getName() + "\".Company where name like ? ";
            List<Object> args = Lists.newArrayList(name);
            if (companyId != null) {
                sql = sql + " and id=? ";
                args.add(companyId);
            }
            SqlQuery<Long, Company> sqlQuery = new SqlQuery<>(Company.class, sql);
            sqlQuery.setArgs(args.stream().toArray());
            QueryCursor<Cache.Entry<Long, Company>> queryCache = personCache.query(sqlQuery);
            List<Cache.Entry<Long, Company>> result = queryCache.getAll();
            result.forEach(item -> personList.add(item.getValue()));
        }
        return personList;
    }

    @PostConstruct
    public void initCachePersonConfig() {
        //因为cache是基于键值对（key-value pair）的存储机制，其中key和value都应该是预定义类型的实例。
        //所以，一个Ignite cache被设计为存储相同类型的Java对象。为每个cache定义一个特定的key和value类型，
        // 以确保数据的一致性和查询效率
        this.configurationPerson = new CacheConfiguration<>(STORE_CACHE_NAME + "_" + Person.class.getSimpleName().toUpperCase());
        this.configurationPerson.setStoreKeepBinary(true);
        this.configurationPerson.setCacheMode(CacheMode.PARTITIONED);
        this.configurationPerson.setAtomicityMode(CacheAtomicityMode.TRANSACTIONAL);
        this.configurationPerson.setIndexedTypes(Long.class, Person.class);
    }

    @PostConstruct
    public void initCacheCompanyConfig() {
        this.configurationCompany = new CacheConfiguration<>(STORE_CACHE_NAME + "_" + Company.class.getSimpleName().toUpperCase());
        this.configurationCompany.setStoreKeepBinary(true);
        this.configurationCompany.setCacheMode(CacheMode.PARTITIONED);
        this.configurationCompany.setAtomicityMode(CacheAtomicityMode.TRANSACTIONAL);
        this.configurationCompany.setIndexedTypes(Long.class, Company.class);
    }
}
