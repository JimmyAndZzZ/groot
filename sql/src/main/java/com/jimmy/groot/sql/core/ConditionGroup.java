package com.jimmy.groot.sql.core;

import com.jimmy.groot.sql.enums.ConditionTypeEnum;
import lombok.Data;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.List;

@Data
public class ConditionGroup implements Serializable {

    private List<Condition> conditions = new ArrayList<>();

    private ConditionTypeEnum conditionTypeEnum = ConditionTypeEnum.AND;
}
