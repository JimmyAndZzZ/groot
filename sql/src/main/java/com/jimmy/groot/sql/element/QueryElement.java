package com.jimmy.groot.sql.element;

import com.jimmy.groot.sql.core.AggregateFunction;
import lombok.Data;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

@Data
public class QueryElement implements Serializable {

    private int start;

    private int end;

    private boolean isAllColumn = true;

    private boolean isSelectAll = false;

    private boolean isWithoutCondition = false;

    private Set<String> select = new HashSet<>();

    private Set<String> needColumnNames = new HashSet<>();

    private List<ConditionElement> conditionElements = new ArrayList<>();

    private List<AggregateFunction> aggregateFunctions = new ArrayList<>();
}
