package com.ignite.project.demo.cache;

import com.ignite.project.demo.affinity.Company;
import com.ignite.project.demo.affinity.Person;
import lombok.Data;

import java.util.List;

@Data
public class CompanyPersonDto {

    private Company company;

    private List<Person> persons;


}
