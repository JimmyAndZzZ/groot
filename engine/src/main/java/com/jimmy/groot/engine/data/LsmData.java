package com.jimmy.groot.engine.data;

import com.jimmy.groot.engine.data.lsm.SsTable;
import com.jimmy.groot.engine.metadata.Column;
import com.jimmy.groot.sql.core.QueryPlus;

import java.util.*;
import java.util.concurrent.locks.ReadWriteLock;

public class LsmData extends AbstractData {

    public LsmData(List<Column> columns) {
        super(columns);
    }

    @Override
    public void save(Map<String, Object> doc) {

    }

    @Override
    public void remove(Map<String, Object> doc) {

    }

    @Override
    public Collection<Map<String, Object>> page(QueryPlus queryPlus, Integer pageNo, Integer pageSize) {
        return null;
    }

    @Override
    public Collection<Map<String, Object>> list(QueryPlus queryPlus) {
        return null;
    }
}
