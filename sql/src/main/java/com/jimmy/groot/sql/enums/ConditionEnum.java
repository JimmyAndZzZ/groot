package com.jimmy.groot.sql.enums;

import lombok.AllArgsConstructor;
import lombok.Getter;

@Getter
@AllArgsConstructor
public enum ConditionEnum {

    EQ(true),
    GT(false),
    GE(false),
    LE(false),
    LT(false),
    IN(true),
    NOT_IN(true),
    LIKE(false),
    NULL(false),
    NOT_NULL(false),
    NE(false),
    NOT_LIKE(false),
    RANGER(false);

    private Boolean isNeedIndex;
}
