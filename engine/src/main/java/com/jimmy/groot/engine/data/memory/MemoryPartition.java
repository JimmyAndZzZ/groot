package com.jimmy.groot.engine.data.memory;

import com.google.common.collect.Maps;
import lombok.Getter;

import java.util.Collection;
import java.util.Map;

public class MemoryPartition {

    @Getter
    private String code;

    @Getter
    private Map<String, Object> key;

    private Map<String, MemoryFragment> partitions = Maps.newConcurrentMap();

    public MemoryPartition(String code, Map<String, Object> key) {
        this.code = code;
        this.key = key;
    }

    public int count() {
        return partitions.size();
    }

    public MemoryFragment getFragmentByUniqueCode(String uniqueCode) {
        return this.partitions.get(uniqueCode);
    }

    public Collection<MemoryFragment> getFragments() {
        return partitions.values();
    }

    public void save(String code, MemoryFragment memoryFragment) {
        partitions.put(code, memoryFragment);
    }

    public void remove(String code) {
        partitions.remove(code);
    }

}
