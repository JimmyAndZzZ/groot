package com.jimmy.groot.engine.core;

import cn.hutool.core.map.MapUtil;
import cn.hutool.crypto.SecureUtil;
import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import com.jimmy.groot.boot.exception.EngineException;
import lombok.Data;
import lombok.Getter;

import java.io.Serializable;
import java.util.List;
import java.util.Map;
import java.util.TreeMap;

public class Partition {

    @Getter
    private final String code;

    @Getter
    private final Map<String, Object> key;

    private final List<Fragment> fragments = Lists.newArrayList();

    public Partition(Map<String, Object> partitionKeys) {
        if (MapUtil.isEmpty(partitionKeys)) {
            throw new EngineException("分区键为空");
        }
        // 将 Map 按 ASCII 码排序
        Map<String, Object> sortedMap = new TreeMap<>(partitionKeys);

        StringBuilder sb = new StringBuilder();
        for (Map.Entry<String, Object> entry : sortedMap.entrySet()) {
            sb.append(entry.getKey()).append("=").append(entry.getValue().toString()).append("&");
        }

        this.key = partitionKeys;
        this.code = SecureUtil.md5(sb.toString());
    }

}
