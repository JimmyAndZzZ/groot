package com.jimmy.groot.engine.data.other;

import lombok.Data;

import java.util.Map;

@Data
public class IndexData {

    private final String key;

    private final Map<String, Object> data;

    public IndexData(String key, Map<String, Object> data) {
        this.key = key;
        this.data = data;
    }
}
