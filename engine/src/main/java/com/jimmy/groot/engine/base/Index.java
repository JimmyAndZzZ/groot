package com.jimmy.groot.engine.base;

import com.jimmy.groot.engine.core.index.Unique;
import com.jimmy.groot.sql.enums.IndexTypeEnum;

import java.io.Serializable;
import java.util.Map;
import java.util.Set;

public interface Index extends Serializable {

    IndexTypeEnum type();

    Map<String, Object> getKey(String code);

    Map<String, Object> getValue(String code);

    void remove(Map<String, Object> doc);

    void save(Map<String, Object> doc);

    Unique getUnique(Map<String, Object> doc);

    String getCode(Map<String, Object> data);

    Set<String> getColumns();
}
