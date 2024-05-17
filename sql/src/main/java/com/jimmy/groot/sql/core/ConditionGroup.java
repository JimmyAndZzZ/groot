package com.jimmy.groot.sql.core;

import lombok.Data;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.List;

@Data
public class ConditionGroup implements Serializable {

    private List<Condition> conditions = new ArrayList<>();

}
