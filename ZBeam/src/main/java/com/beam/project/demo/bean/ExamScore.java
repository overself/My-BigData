package com.beam.project.demo.bean;

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
}
