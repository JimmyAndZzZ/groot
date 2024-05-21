package com.jimmy.groot.engine.core.index;

import com.google.common.collect.Maps;
import com.jimmy.groot.engine.base.Index;
import com.jimmy.groot.engine.exception.EngineException;

import java.util.Map;
import java.util.Set;

public abstract class BaseIndex implements Index {

    protected Unique getUnique(Map<String, Object> doc, Set<String> columns) {
        Map<String, Object> uniqueData = Maps.newHashMap();

        for (String uniqueKey : columns) {
            Object o = doc.get(uniqueKey);
            if (o == null) {
                throw new EngineException("主键为空,主键名:" + uniqueKey);
            }

            uniqueData.put(uniqueKey, o);
        }

        Unique unique = new Unique();
        unique.setUniqueData(uniqueData);
        unique.setCode(this.getCode(uniqueData));
        return unique;
    }
}
