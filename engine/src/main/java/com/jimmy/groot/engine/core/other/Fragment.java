package com.jimmy.groot.engine.core.other;

import cn.hutool.core.collection.CollUtil;
import cn.hutool.core.map.MapUtil;
import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import com.jimmy.groot.engine.store.SegmentPool;
import com.jimmy.groot.engine.store.SegmentSerializer;
import lombok.Data;
import lombok.Getter;

import java.io.Serializable;
import java.util.List;
import java.util.Map;

public class Fragment implements Serializable {

    private Integer diskIndex;

    private SegmentSerializer segmentSerializer;

    @Getter
    private Map<String, Object> key = Maps.newHashMap();

    private List<Integer> memoryIndex = Lists.newArrayList();

    private Fragment() {

    }

    public static Fragment build(SegmentSerializer segmentSerializer, Map<String, Object> key) {
        Fragment fragment = new Fragment();
        fragment.key = key;
        fragment.segmentSerializer = segmentSerializer;
        return fragment;
    }

    public Fragment writeDisk(Map<String, Object> data) {
        if (MapUtil.isNotEmpty(data)) {
            this.diskIndex = SegmentPool.getInstance().allocateFromDisk(segmentSerializer.serialize(data));
        }

        return this;
    }

    public Fragment writeMemory(Map<String, Object> data) {
        if (MapUtil.isNotEmpty(data)) {
            this.memoryIndex = SegmentPool.getInstance().allocateFromMemory(segmentSerializer.serialize(data));
        }

        return this;
    }

    public Map<String, Object> getData() {
        Map<String, Object> data = Maps.newHashMap();

        if (diskIndex != null) {
            data.putAll(segmentSerializer.deserialize(SegmentPool.getInstance().get(diskIndex)));
        }

        if (CollUtil.isNotEmpty(memoryIndex)) {
            data.putAll(segmentSerializer.deserialize(SegmentPool.getInstance().get(memoryIndex)));
        }

        return data;
    }
}
