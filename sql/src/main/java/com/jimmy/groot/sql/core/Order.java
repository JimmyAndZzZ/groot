package com.jimmy.groot.sql.core;

import lombok.Data;

import java.io.Serializable;

@Data
public class Order implements Serializable {

    private String fieldName;

    private Boolean isDesc = false;
}
