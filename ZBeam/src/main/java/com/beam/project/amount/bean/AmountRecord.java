package com.beam.project.amount.bean;

import lombok.Data;

import java.io.Serializable;
import java.time.LocalDate;

@Data
public class AmountRecord implements Serializable {

    private String code;

    private String subCode;

    private String type;

    private Long amount;

    private LocalDate createDate;

}
