package com.beam.project.demo.bean;

import lombok.Data;
import org.apache.beam.sdk.options.Default;

import java.io.Serializable;

@Data
public class Student implements Cloneable, Serializable {

    private String code;

    private String name;

    private String classCode;

    private String className;

    public Student() {
    }

    public Student(String code, String name, String classCode) {
        this.code = code;
        this.name = name;
        this.classCode = classCode;
    }

    @Override
    public Student clone() throws CloneNotSupportedException {
        return (Student) super.clone();
    }
}
