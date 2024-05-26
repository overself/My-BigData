package com.ignite.project.demo;

import cn.hutool.core.lang.Snowflake;
import cn.hutool.core.util.IdUtil;
import com.ignite.project.BootBaseTest;
import com.ignite.project.demo.affinity.Company;
import com.ignite.project.demo.affinity.Person;
import com.ignite.project.demo.cache.CompanyPersonDao;
import com.ignite.project.demo.cache.CompanyPersonDto;
import com.ignite.project.demo.cache.PersonServiceBizImpl;
import lombok.extern.slf4j.Slf4j;
import org.apache.ignite.Ignite;
import org.assertj.core.util.Lists;
import org.junit.jupiter.api.MethodOrderer;
import org.junit.jupiter.api.Order;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.TestMethodOrder;

import javax.annotation.Resource;
import java.util.List;

@Slf4j
@TestMethodOrder(value = MethodOrderer.OrderAnnotation.class)
public class PersonServiceBizTest extends BootBaseTest {


    private Snowflake snowFlake = IdUtil.createSnowflake(0, 1);

    private CompanyPersonDto companyPersonDto;

    @Resource
    private PersonServiceBizImpl personServiceBiz;

    @Resource
    private Ignite ignite;

    @Test
    @Order(1)
    public void createCompanyPersonTest() {
        log.info("createCompanyPersonTest");
        this.companyPersonDto = newCompanyPerson();
        personServiceBiz.createPerson(this.companyPersonDto);
        log.info("公司及用户已创建！");
    }

    @Test
    @Order(2)
    public void getCompanyPersonTest1() {
        log.info("getCompanyPersonTest1 第一次查询");
        long id = this.companyPersonDto.getPersons().get(2).getId();
        Person person = personServiceBiz.loadPerson(id);
        log.info("person ={} : {}", person.getId(), person);
    }

    @Test
    @Order(3)
    public void getCompanyPersonTest2() {
        log.info("getCompanyPersonTest1 第二次查询");
        long id = this.companyPersonDto.getPersons().get(2).getId();
        Person person = personServiceBiz.loadPerson(id);
        log.info("person ={} : {}", person.getId(), person);
    }

    @Test
    @Order(4)
    public void updateCompanyPersonTest() {
        log.info("updateCompanyPersonTest 修改数据");
        Person person = this.companyPersonDto.getPersons().get(2);
        person.setName(person.getName() + "=>修改一下名称");
        person = personServiceBiz.updatePerson(person);
        log.info("person ={} : {}", person.getId(), person);
    }

    @Test
    @Order(5)
    public void updateAfterGetCompanyPersonTest1() {
        log.info("updateAfterGetCompanyPersonTest1 修改后 再查询 第三次");
        long id = this.companyPersonDto.getPersons().get(2).getId();
        Person person = personServiceBiz.loadPerson(id);
        log.info("person ={} : {}", person.getId(), person);
    }

    @Test
    @Order(6)
    public void updateAfterGetCompanyPersonTest2() {
        log.info("updateAfterGetCompanyPersonTest2 修改后 再查询 第四次");
        long id = this.companyPersonDto.getPersons().get(2).getId();
        Person person = personServiceBiz.loadPerson(id);
        log.info("person ={} : {}", person.getId(), person);
    }


    @Test
    @Order(7)
    public void getPersonListOfCompanyTest1() {
        log.info("getPersonListOfCompanyTest1 查询公司内姓张的用户 第一次");
        long companyId = this.companyPersonDto.getCompany().getId();
        List<Person> persons = personServiceBiz.getPersonListOfCompany(companyId, "张");
        for (Person person : persons) {
            log.info("person ={} : {}", person.getId(), person);
        }
    }

    @Test
    @Order(8)
    public void getPersonListOfCompanyTest2() {
        log.info("getPersonListOfCompanyTest2 查询公司内姓张的用户 第二次");
        long companyId = this.companyPersonDto.getCompany().getId();
        List<Person> persons = personServiceBiz.getPersonListOfCompany(companyId, "张");
        for (Person person : persons) {
            log.info("person ={} : {}", person.getId(), person);
        }
    }

    @Test
    @Order(9)
    public void deleteCompanyPersonTest1() {
        log.info("deleteCompanyPersonTest1 删除用户");
        long id = this.companyPersonDto.getPersons().get(2).getId();
        personServiceBiz.deletePerson(id);
        log.info("person ={} : 已删除", id);
    }

    @Test
    @Order(10)
    public void getDeleteAfterCompanyPersonTest5() {
        log.info("getDeleteAfterCompanyPersonTest1 删除后 再查询 第五次");
        long id = this.companyPersonDto.getPersons().get(2).getId();
        Person person = personServiceBiz.loadPerson(id);
        log.info("person ={} : {}", id, person);
    }

    @Test
    @Order(11)
    public void getDeleteAfterPersonListOfCompanyTest1() {
        log.info("PersonListOfCompanyTest3 删除后 再查询 公司内姓张的用户 第三次");
        long companyId = this.companyPersonDto.getCompany().getId();
        List<Person> persons = personServiceBiz.getPersonListOfCompany(companyId, "张");
        for (Person person : persons) {
            log.info("公司内姓张的用户 ={} : {}", person.getId(), person);
        }
    }

    @Test
    @Order(12)
    public void companyAllTest() {
        log.info("companyAllTest 查询公司");
        Company company = personServiceBiz.loadCompany(this.companyPersonDto.getCompany().getId());
        log.info("修改前公司：{}", company);
        company.setName("修改公司名测试=>" + company.getName());
        company = personServiceBiz.updateCompany(company);
        log.info("修改后直接返回：{}", company);
        List<Company> companies = personServiceBiz.getCompanyList(null, "Company");
        log.info("修改后查询结果：{}", company);
        for (Company companyTmp : companies) {
            log.info("Company : {}", companyTmp);
        }
    }

    @Test
    @Order(13)
    public void deleteAllTest() {
        log.info("PersonListOfCompanyTest3 查询公司内姓张的用户 第三次");
        long companyId = this.companyPersonDto.getCompany().getId();
        List<Person> persons = this.companyPersonDto.getPersons();
        persons.remove(this.companyPersonDto.getPersons().get(2));
        for (Person person : persons) {
            personServiceBiz.deletePerson(person.getId());
            log.info("person ={} : 已删除", person.getId());
        }
        personServiceBiz.deleteCompany(companyId);
        log.info("company ={} : 已删除", companyId);
    }

    @Test
    @Order(14)
    public void createCompanyPersonExceptionTest() {
        log.info("PersonListOfCompanyTest3 查询公司内姓张的用户 第三次");
        CompanyPersonDto companyPersonDto = newCompanyPerson();
        Company company = companyPersonDto.getCompany();
        Person person6 = new Person(snowFlake.nextId(), "欧阳峰", company.getId(), null, "瘦客户端事务管理器配置");
        companyPersonDto.getPersons().add(person6);
        try {
            personServiceBiz.createPerson(companyPersonDto);
        } catch (Exception ex) {
            log.error(ex.getLocalizedMessage(), ex);
        }
        Company companyTmp = personServiceBiz.loadCompany(company.getId());
        log.info("Transactional Company.Tmp={}", companyTmp);
        Person personTmp = personServiceBiz.loadPerson(companyPersonDto.getPersons().get(2).getId());
        log.info("Transactional Persons[2].Tmp={}", personTmp);
        personTmp = personServiceBiz.loadPerson(person6.getId());
        log.info("Transactional Persons[6].Tmp={}", personTmp);
    }

    @Test
    @Order(15)
    public void createDestroyCacheTest() {
        log.info("createDestroyCacheTest 销毁缓存");
        ignite.destroyCache(CompanyPersonDao.STORE_CACHE_NAME);
    }


    private CompanyPersonDto newCompanyPerson() {
        CompanyPersonDto companyPersonDto = new CompanyPersonDto();
        Company company = new Company(snowFlake.nextId(), "WenJay Company Test");
        companyPersonDto.setCompany(company);

        List<Person> persons = Lists.newArrayList();
        Person person0 = new Person(snowFlake.nextId(), "张三", company.getId(), null, "该方法事务管理器会根据提供的配置自动启动一个");
        persons.add(person0);
        Person person1 = new Person(snowFlake.nextId(), "张四", company.getId(), null, "胖客户端事务管理器配置");
        persons.add(person1);
        Person person2 = new Person(snowFlake.nextId(), "张无忌", company.getId(), null, "多种方式混用是不正确的，会导致事务管理器启动时抛出异常。");
        persons.add(person2);
        Person person3 = new Person(snowFlake.nextId(), "李斯", company.getId(), null, "使用原生客户端接入集群，接入集群配置和事务并发模型配置");
        persons.add(person3);
        Person person4 = new Person(snowFlake.nextId(), "李逵", company.getId(), null, "事务并发模型配置定义了事务管理器将应用于其处理的所有事务");
        persons.add(person4);
        Person person5 = new Person(snowFlake.nextId(), "李民", company.getId(), null, "如果未配置事务并发模型，则会使用PESSIMISTIC并发模型");
        persons.add(person5);
        companyPersonDto.setPersons(persons);
        return companyPersonDto;
    }

}
