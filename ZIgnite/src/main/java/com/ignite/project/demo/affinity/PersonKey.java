package com.ignite.project.demo.affinity;

import lombok.AllArgsConstructor;
import lombok.Data;
import org.apache.ignite.cache.affinity.AffinityKeyMapped;

import java.io.Serializable;

@Data
@AllArgsConstructor
public class PersonKey implements Serializable {

    private Long personId;

    @AffinityKeyMapped
    private Long companyId;

    private Long roleId;

}
