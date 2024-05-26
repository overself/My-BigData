package com.ignite.project.demo.affinity;

import lombok.AllArgsConstructor;
import lombok.Data;
import org.apache.ignite.cache.query.annotations.QuerySqlField;

import java.io.Serializable;

@Data
@AllArgsConstructor
public class Company implements Serializable {

    @QuerySqlField(index = true)
    private Long id;

    @QuerySqlField
    private String name;

}
