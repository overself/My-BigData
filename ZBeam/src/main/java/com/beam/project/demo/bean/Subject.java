package com.beam.project.demo.bean;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonIgnore;
import com.fasterxml.jackson.annotation.JsonValue;

import java.io.Serializable;

public enum Subject implements Serializable {

    mathematics("maths", "maths"),

    physics("physics", "physics"),

    chemistry("chemistry", "chemistry"),

    English("English", "English"),

    Chinese("Chinese", "Chinese");

    private String code;

    private String description;

    Subject(String code, String description) {
        this.code = code;
        this.description = description;
    }

    @JsonCreator
    public static Subject fromCode(String code) {
        for (Subject e : values()) {
            if (e.code.equals(code)) {
                return e;
            }
        }
        return null;
    }

    @JsonValue
    public String getValue() {
        return this.code;
    }

    @JsonIgnore
    public String getDescription() {
        return this.description;
    }
}
