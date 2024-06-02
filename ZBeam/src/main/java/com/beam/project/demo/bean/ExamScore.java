package com.beam.project.demo.bean;

import com.fasterxml.jackson.annotation.JsonIgnore;
import lombok.Data;
import org.joda.time.Instant;

import java.io.Serializable;
import java.math.BigDecimal;
import java.util.Date;

@Data
public class ExamScore implements Serializable, Cloneable {

    private Long dataId;

    private String schoolCode;

    private String schoolName;

    private String classCode;

    private String studentCode;

    private Subject subject;

    private BigDecimal score;

    private Date examTime;

    private Instant timestamp;

    @Override
    public ExamScore clone() throws CloneNotSupportedException {
        return (ExamScore) super.clone();
    }

    @JsonIgnore
    public String getClassSubjectKey() {
        return schoolCode + ":" + classCode + ":" + subject.getValue();
    }

    @JsonIgnore
    public String getSchoolSubjectKey() {
        return schoolCode + ":" + subject.getValue();
    }

    @Override
    public String toString() {
        return "ExamScore[" +
                "schoolCode='" + schoolCode + '\'' +
                ", classCode='" + classCode + '\'' +
                ", subject=" + subject +
                ", score=" + score +
                ", studentCode='" + studentCode + '\'' +
                ']';
    }
}
