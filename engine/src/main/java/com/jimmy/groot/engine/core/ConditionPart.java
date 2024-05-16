package com.jimmy.groot.engine.core;

import lombok.Data;

import java.io.Serializable;

@Data
public class ConditionPart implements Serializable {

    private String expression;
}
