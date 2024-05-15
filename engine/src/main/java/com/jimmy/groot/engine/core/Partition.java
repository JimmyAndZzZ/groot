package com.jimmy.groot.engine.core;

import com.google.common.collect.Maps;
import com.jimmy.groot.sql.core.Wrapper;
import com.jimmy.groot.sql.exception.EngineException;
import com.jimmy.groot.engine.store.SegmentPool;
import com.jimmy.groot.engine.store.SegmentSerializer;

import java.util.Map;
import java.util.Set;


public class Partition extends Element {

    private final Set<String> uniqueKeys;

    private final SegmentSerializer segmentSerializer;

    private final Map<String, Fragment> fragments = Maps.newHashMap();

    public Partition(Set<String> uniqueKeys, SegmentSerializer segmentSerializer) {
        this.uniqueKeys = uniqueKeys;
        this.segmentSerializer = segmentSerializer;
    }

    public void remove(Wrapper wrapper) {

    }

    public void save(Map<String, Object> doc) {
        Map<String, Object> uniqueData = Maps.newHashMap();

        for (String uniqueKey : uniqueKeys) {
            Object o = doc.get(uniqueKey);
            if (o == null) {
                throw new EngineException("主键为空,主键名:" + uniqueKey);
            }

            uniqueData.put(uniqueKey, o);
        }

        Fragment fragment = new Fragment();
        fragment.setKey(uniqueData);
        fragment.setIndex(SegmentPool.getInstance().allocate(segmentSerializer.serialize(doc)));
        this.fragments.put(this.getCode(doc), fragment);
    }

}
