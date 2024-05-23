package com.jimmy.groot.sql.core;

import cn.hutool.core.util.ArrayUtil;
import com.jimmy.groot.sql.enums.ConditionTypeEnum;
import lombok.Data;

import java.io.Serializable;
import java.util.*;

@Data
public class QueryPlus implements Serializable {

    private List<ConditionGroup> conditionGroups = new ArrayList<>();

    private List<Order> orders = new ArrayList<>();

    private Set<String> select = new HashSet<>();

    private List<String> groupBy = new ArrayList<>();

    private List<AggregateFunction> aggregateFunctions = new ArrayList<>();

    protected QueryPlus() {

    }

    QueryPlus addGroup(List<Condition> conditions) {
        ConditionGroup conditionGroup = new ConditionGroup();
        conditionGroup.setConditions(conditions);
        conditionGroups.add(conditionGroup);
        return this;
    }

    QueryPlus addAggregateFunction(AggregateFunction aggregateFunction) {
        this.aggregateFunctions.add(aggregateFunction);
        return this;
    }

    QueryPlus addOrder(Order order) {
        orders.add(order);
        return this;
    }

    QueryPlus select(String... columns) {
        if (ArrayUtil.isNotEmpty(columns)) {
            select.addAll(Arrays.asList(columns));
        }

        return this;
    }

    QueryPlus groupBy(String... columns) {
        if (ArrayUtil.isNotEmpty(columns)) {
            Collections.addAll(groupBy, columns);
        }

        return this;
    }
}
