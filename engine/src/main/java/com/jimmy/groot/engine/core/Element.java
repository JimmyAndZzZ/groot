package com.jimmy.groot.engine.core;

import cn.hutool.core.map.MapUtil;
import cn.hutool.crypto.SecureUtil;

import java.io.Serializable;
import java.util.Map;
import java.util.TreeMap;

public class Element implements Serializable {

    /**
     * 获取唯一值
     *
     * @param data
     * @return
     */
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


}
