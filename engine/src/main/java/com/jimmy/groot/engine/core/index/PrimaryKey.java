package com.jimmy.groot.engine.core.index;

import cn.hutool.core.map.MapUtil;
import cn.hutool.crypto.SecureUtil;
import com.google.common.collect.Maps;
import com.google.common.collect.Sets;
import com.jimmy.groot.engine.base.Index;
import com.jimmy.groot.engine.core.Fragment;
import com.jimmy.groot.engine.core.Unique;
import com.jimmy.groot.engine.exception.EngineException;
import com.jimmy.groot.engine.store.SegmentPool;
import com.jimmy.groot.engine.store.SegmentSerializer;
import com.jimmy.groot.sql.enums.IndexTypeEnum;
import lombok.Getter;

import java.util.Map;
import java.util.Set;
import java.util.TreeMap;

public class PrimaryKey implements Index {

    private final SegmentSerializer segmentSerializer;

    @Getter
    private final Set<String> columns = Sets.newHashSet();

    private final Map<String, Fragment> fragments = Maps.newHashMap();

    public PrimaryKey(SegmentSerializer segmentSerializer) {
        this.segmentSerializer = segmentSerializer;
    }

    public void remove(Map<String, Object> doc) {
        Unique unique = this.getUnique(doc);
        fragments.remove(unique.getCode());
    }

    public void save(Map<String, Object> doc) {
        Unique unique = this.getUnique(doc);

        Fragment fragment = new Fragment();
        fragment.setKey(unique.getUniqueData());
        fragment.setIndex(SegmentPool.getInstance().allocate(segmentSerializer.serialize(doc)));
        this.fragments.put(unique.getCode(), fragment);
    }

    protected Unique getUnique(Map<String, Object> doc) {
        Map<String, Object> uniqueData = Maps.newHashMap();

        for (String uniqueKey : this.getColumns()) {
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

    protected String getCode(Map<String, Object> data) {
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
