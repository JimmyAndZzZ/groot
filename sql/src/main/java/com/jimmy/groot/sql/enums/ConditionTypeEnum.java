package com.jimmy.groot.sql.enums;

import lombok.AllArgsConstructor;
import lombok.Getter;

@Getter
@AllArgsConstructor
public enum ConditionTypeEnum {

    AND("&&"), OR("||");

    private String expression;
}
