package com.jimmy.groot.sql.element;

import lombok.Data;

import java.io.Serializable;
import java.util.HashSet;
import java.util.Set;

@Data
public class QueryElement implements Serializable {

    private int start;

    private int end;

    private boolean isAllColumn = true;

    private boolean isSelectAll = false;

    private boolean isWithoutCondition = false;

    private ConditionElement conditionElement;

    private Set<String> select = new HashSet<>();

    private Set<String> needColumnNames = new HashSet<>();
}
