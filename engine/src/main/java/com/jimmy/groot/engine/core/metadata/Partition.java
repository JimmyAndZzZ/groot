package com.jimmy.groot.engine.core.metadata;

import com.google.common.collect.Maps;
import com.jimmy.groot.engine.core.other.Fragment;
import lombok.Getter;

import java.util.Map;

public class Partition {

    @Getter
    private Map<String, Object> key;

    private Map<String, Fragment> partitions = Maps.newConcurrentMap();

    public Partition(Map<String, Object> key) {
        this.key = key;
    }

    public void save(String code, Fragment fragment) {
        partitions.put(code, fragment);
    }

    public void remove(String code) {
        partitions.remove(code);
    }

}
