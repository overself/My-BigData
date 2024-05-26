package com.ignite.project.demo.binary;

import lombok.AllArgsConstructor;
import lombok.Data;
import org.apache.ignite.cache.query.annotations.QuerySqlField;

@Data
@AllArgsConstructor
public class BinaryPojo {

    /** Primary key. */
    @QuerySqlField(index = true)
    private int id;

    private String name;

    private String context;
}
