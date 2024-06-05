package com.beam.project.demo.bean;

import com.fasterxml.jackson.annotation.JsonIgnore;
import lombok.Data;
import org.joda.time.Instant;

import java.io.Serializable;
import java.util.Objects;

@Data
public class SchoolClass implements Serializable, Cloneable {

    private Long dataId;

    private String schoolCode;

    private String classCode;

    private Instant timestamp;

    @Override
    public SchoolClass clone() throws CloneNotSupportedException {
        return (SchoolClass) super.clone();
    }

    @JsonIgnore
    public String getSchoolClassKey() {
        return schoolCode + ":" + classCode;
    }


    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        SchoolClass that = (SchoolClass) o;
        return Objects.equals(schoolCode, that.schoolCode) &&
                Objects.equals(classCode, that.classCode);
    }

    @Override
    public int hashCode() {
        return Objects.hash(schoolCode, classCode);
    }

    @Override
    public String toString() {
        return "SchoolClass[" + "school='" + schoolCode + '\'' +
                ", class='" + classCode + '\'' + ']';
    }
}