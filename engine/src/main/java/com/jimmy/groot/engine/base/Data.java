package com.jimmy.groot.engine.base;

import com.jimmy.groot.sql.core.QueryPlus;
import com.jimmy.groot.sql.element.QueryElement;

import java.util.Collection;
import java.util.Map;

public interface Data {

    void save(Map<String, Object> doc);

    void remove(Map<String, Object> doc);

    Collection<Map<String, Object>> query(QueryElement queryElement);
}
