package com.jimmy.groot.engine.data.memory;

import cn.hutool.core.collection.CollUtil;
import cn.hutool.core.map.MapUtil;
import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import com.jimmy.groot.platform.base.Serializer;
import lombok.Getter;

import java.io.Serializable;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

public class MemoryFragment implements Serializable {

    @Getter
    private String code;

    private Serializer serializer;

    @Getter
    private Map<String, Object> key = Maps.newHashMap();

    private List<Integer> memoryIndex = Lists.newArrayList();

    private MemoryFragment() {

    }

    public static MemoryFragment build(String code, Serializer serializer, Map<String, Object> key) {
        MemoryFragment memoryFragment = new MemoryFragment();
        memoryFragment.key = key;
        memoryFragment.code = code;
        memoryFragment.serializer = serializer;
        return memoryFragment;
    }

    public MemoryFragment writeMemory(Map<String, Object> data) {
        if (MapUtil.isNotEmpty(data)) {
            this.memoryIndex = MemoryPool.getInstance().allocateFromMemory(serializer.serialize(data));
        }

        return this;
    }

    public Map<String, Object> getData() {
        Map<String, Object> data = Maps.newHashMap();

        if (CollUtil.isNotEmpty(memoryIndex)) {
            data.putAll(serializer.deserialize(MemoryPool.getInstance().get(memoryIndex), HashMap.class));
        }

        return data;
    }
}
