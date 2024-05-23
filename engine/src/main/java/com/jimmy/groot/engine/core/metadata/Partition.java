package com.jimmy.groot.engine.core.metadata;

import com.google.common.collect.Maps;
import com.jimmy.groot.engine.core.other.Fragment;
import lombok.Getter;

import java.util.Collection;
import java.util.Map;

public class Partition {

    @Getter
    private String code;

    @Getter
    private Map<String, Object> key;

    private Map<String, Fragment> partitions = Maps.newConcurrentMap();

    public Partition(String code, Map<String, Object> key) {
        this.code = code;
        this.key = key;
    }

    public int count() {
        return partitions.size();
    }

    public Fragment getFragmentByUniqueCode(String uniqueCode) {
        return this.partitions.get(uniqueCode);
    }

    public Collection<Fragment> getFragments() {
        return partitions.values();
    }

    public void save(String code, Fragment fragment) {
        partitions.put(code, fragment);
    }

    public void remove(String code) {
        partitions.remove(code);
    }

}
