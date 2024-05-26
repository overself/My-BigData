package com.ignite.project.demo.cache;

import com.ignite.project.demo.affinity.Company;
import com.ignite.project.demo.affinity.Person;
import org.springframework.stereotype.Service;
import org.springframework.transaction.annotation.Transactional;

import javax.annotation.Resource;
import java.util.List;

@Service
public class PersonServiceBizImpl {

    @Resource
    private CompanyPersonDao dao;

    //@Transactional(rollbackFor = Exception.class)
    public boolean createPerson(CompanyPersonDto companyPersonDto) {
        Long companyId = dao.saveCompany(companyPersonDto.getCompany());
        List<Person> personList = companyPersonDto.getPersons();
        int count = 1;
        for (Person item : personList) {
            if (count++ > 6) {
                throw new RuntimeException("Person count > 6");
            }
            item.setCompanyId(companyId);
            dao.savePerson(item);
        }
        return true;
    }

    public Person loadPerson(Long personId) {
        return dao.getPersonById(personId);
    }

    //@Transactional(rollbackFor = Exception.class)
    public Person updatePerson(Person person) {
        return dao.updatePerson(person);
    }

    //@Transactional(rollbackFor = Exception.class)
    public boolean deletePerson(Long personId) {
        return dao.deletePerson(personId);
    }

    public Company loadCompany(Long companyId) {
        return dao.getCompanyById(companyId);
    }

    //@Transactional(rollbackFor = Exception.class)
    public Company updateCompany(Company company) {
        return dao.updateCompany(company);
    }

    //@Transactional(rollbackFor = Exception.class)
    public boolean deleteCompany(Long companyId) {
        return dao.deleteCompany(companyId);
    }

    public List<Person> getPersonListOfCompany(Long companyId, String personName) {
        return dao.getPersonListOfCompany(companyId, personName);
    }

    public List<Company> getCompanyList(Long companyId, String name) {
        return dao.getCompanyList(name, companyId);
    }
}
