package com.jimmy.groot.engine.metadata;

import com.google.common.collect.Sets;
import lombok.Getter;

import java.io.Serializable;
import java.util.Set;

@Getter
public class Index implements Serializable {

    @Getter
    private final String name;

    private final Set<String> columns = Sets.newHashSet();

    public Index(String name) {
        this.name = name;
    }

    public void addColumn(String columnName) {
        this.columns.add(columnName);
    }

    public boolean contain(String columnName) {
        return this.columns.contains(columnName);
    }

    public boolean isEmpty() {
        return this.columns.isEmpty();
    }

    public int size() {
        return columns.size();
    }
}
