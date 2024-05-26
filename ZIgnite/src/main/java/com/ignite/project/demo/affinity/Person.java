package com.ignite.project.demo.affinity;

import lombok.AllArgsConstructor;
import lombok.Data;
import org.apache.ignite.cache.query.annotations.QuerySqlField;
import org.apache.ignite.cache.query.annotations.QueryTextField;

import java.io.Serializable;

@Data
@AllArgsConstructor
public class Person implements Serializable {

    @QuerySqlField(index = true)
    private Long id;

    @QuerySqlField
    private String name;

    @QuerySqlField
    private Long companyId;

    @QuerySqlField
    private Long roleId;

    @QueryTextField
    private String describe;

}
