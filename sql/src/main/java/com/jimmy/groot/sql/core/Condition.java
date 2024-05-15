package com.jimmy.groot.sql.core;

import com.jimmy.groot.sql.enums.ConditionEnum;
import com.jimmy.groot.sql.enums.ConditionTypeEnum;
import lombok.Data;

import java.io.Serializable;

@Data
public class Condition implements Serializable {

    private ConditionEnum conditionEnum;

    private String fieldName;

    private Object fieldValue;

    private Object start;

    private Object end;

    private ConditionTypeEnum conditionTypeEnum = ConditionTypeEnum.AND;

    public Condition() {

    }

    public Condition(ConditionEnum conditionEnum, String fieldName, Object fieldValue) {
        this.conditionEnum = conditionEnum;
        this.fieldName = fieldName;
        this.fieldValue = fieldValue;
    }

    public Condition(ConditionEnum conditionEnum, String fieldName, Object start, Object end) {
        this.conditionEnum = conditionEnum;
        this.fieldName = fieldName;
        this.start = start;
        this.end = end;
    }
}
