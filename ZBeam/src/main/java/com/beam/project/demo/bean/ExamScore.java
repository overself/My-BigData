package com.beam.project.demo.bean;

import lombok.Data;

import java.io.Serializable;

@Data
public class ExamScore implements Serializable {

    private String examTime;

    private String studentCode;

    private Subject subject;

    private Integer score;

    private Long timestamp;
}
