package com.beam.project.demo.bean;

import lombok.Data;

import java.io.Serializable;

@Data
public class ClassInfo implements Serializable {

    private String code;

    private String className;

    public ClassInfo() {

    }

    public ClassInfo(String code, String className) {
        this.code = code;
        this.className = className;
    }
}
