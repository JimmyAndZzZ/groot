package com.jimmy.groot.engine.core.index;

import cn.hutool.core.map.MapUtil;
import cn.hutool.crypto.SecureUtil;
import com.google.common.collect.Maps;
import com.jimmy.groot.engine.core.other.Fragment;
import com.jimmy.groot.engine.store.SegmentPool;
import com.jimmy.groot.engine.store.SegmentSerializer;
import com.jimmy.groot.sql.enums.IndexTypeEnum;
import lombok.Getter;

import java.util.Map;
import java.util.Set;
import java.util.TreeMap;

public class PrimaryKey extends BaseIndex {

    @Getter
    private final Set<String> columns;

    private final SegmentSerializer segmentSerializer;

    private final Map<String, Fragment> fragments = Maps.newHashMap();

    public PrimaryKey(SegmentSerializer segmentSerializer, Set<String> columns) {
        this.columns = columns;
        this.segmentSerializer = segmentSerializer;
    }

    @Override
    public Map<String, Object> getKey(String code) {
        Fragment fragment = fragments.get(code);
        return fragment != null ? fragment.getKey() : null;
    }

    @Override
    public Map<String, Object> getValue(String code) {
        Fragment fragment = fragments.get(code);
        return fragment != null ? segmentSerializer.deserialize(SegmentPool.getInstance().get(fragment.getIndex())) : null;
    }

    @Override
    public void remove(Map<String, Object> doc) {
        Unique unique = this.getUnique(doc);
        Fragment remove = fragments.remove(unique.getCode());
        if (remove != null) {
            SegmentPool.getInstance().free(remove.getIndex());
        }
    }

    @Override
    public void save(Map<String, Object> doc) {
        Unique unique = this.getUnique(doc);

        Fragment fragment = new Fragment();
        fragment.setKey(unique.getUniqueData());
        fragment.setIndex(SegmentPool.getInstance().allocate(segmentSerializer.serialize(doc)));
        this.fragments.put(unique.getCode(), fragment);
    }

    @Override
    public Unique getUnique(Map<String, Object> doc) {
        return super.getUnique(doc, this.columns);
    }

    @Override
    public String getCode(Map<String, Object> data) {
        if (MapUtil.isEmpty(data)) {
            return null;
        }
        // 将 Map 按 ASCII 码排序
        Map<String, Object> sortedMap = new TreeMap<>(data);

        StringBuilder sb = new StringBuilder();
        for (Map.Entry<String, Object> entry : sortedMap.entrySet()) {
            sb.append(entry.getKey()).append("=").append(entry.getValue().toString()).append("&");
        }

        return SecureUtil.md5(sb.toString());
    }

    @Override
    public IndexTypeEnum type() {
        return IndexTypeEnum.PRIMARY_KEY;
    }
}
