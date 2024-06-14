package com.jimmy.groot.sql.element;

import com.jimmy.groot.sql.core.Condition;
import lombok.Data;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

@Data
public class ConditionElement implements Serializable {

    private Set<String> uniqueCodes = new HashSet<>();

    private Set<String> partitionCodes = new HashSet<>();

    private List<Condition> conditions = new ArrayList<>();

}
