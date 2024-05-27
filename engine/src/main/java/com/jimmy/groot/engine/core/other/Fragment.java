package com.jimmy.groot.engine.core.other;

import cn.hutool.core.collection.CollUtil;
import cn.hutool.core.map.MapUtil;
import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import com.jimmy.groot.engine.segment.SegmentPool;
import com.jimmy.groot.platform.base.Serializer;
import lombok.Getter;

import java.io.Serializable;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

public class Fragment implements Serializable {

    @Getter
    private String code;

    private Integer diskIndex;

    private Serializer serializer;

    @Getter
    private Map<String, Object> key = Maps.newHashMap();

    private List<Integer> memoryIndex = Lists.newArrayList();

    private Fragment() {

    }

    public static Fragment build(String code, Serializer serializer, Map<String, Object> key) {
        Fragment fragment = new Fragment();
        fragment.key = key;
        fragment.code = code;
        fragment.serializer = serializer;
        return fragment;
    }

    public Fragment writeDisk(Map<String, Object> data) {
        if (MapUtil.isNotEmpty(data)) {
            this.diskIndex = SegmentPool.getInstance().allocateFromDisk(serializer.serialize(data));
        }

        return this;
    }

    public Fragment writeMemory(Map<String, Object> data) {
        if (MapUtil.isNotEmpty(data)) {
            this.memoryIndex = SegmentPool.getInstance().allocateFromMemory(serializer.serialize(data));
        }

        return this;
    }

    public Map<String, Object> getData() {
        Map<String, Object> data = Maps.newHashMap();

        if (diskIndex != null) {
            data.putAll(serializer.deserialize(SegmentPool.getInstance().get(diskIndex), HashMap.class));
        }

        if (CollUtil.isNotEmpty(memoryIndex)) {
            data.putAll(serializer.deserialize(SegmentPool.getInstance().get(memoryIndex), HashMap.class));
        }

        return data;
    }
}
