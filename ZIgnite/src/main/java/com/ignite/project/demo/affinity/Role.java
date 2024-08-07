package com.ignite.project.demo.affinity;

import lombok.AllArgsConstructor;
import lombok.Data;

import java.io.Serializable;

@Data
@AllArgsConstructor
public class Role implements Serializable {

    private Long id;

    private String name;
}
