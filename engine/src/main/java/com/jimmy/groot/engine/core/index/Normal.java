package com.jimmy.groot.engine.core.index;

import com.google.common.collect.Maps;
import com.jimmy.groot.engine.core.Unique;
import com.jimmy.groot.sql.enums.IndexTypeEnum;
import lombok.Data;
import lombok.Getter;

import java.io.Serializable;
import java.util.Map;
import java.util.Set;

public class Normal extends BaseIndex {

    @Getter
    private final Set<String> columns;

    private final PrimaryKey primaryKey;

    private final Map<String, Mapper> indexMapper = Maps.newHashMap();

    public Normal(Set<String> columns, PrimaryKey primaryKey) {
        this.columns = columns;
        this.primaryKey = primaryKey;
    }

    @Override
    public IndexTypeEnum type() {
        return IndexTypeEnum.NORMAL;
    }

    @Override
    public Map<String, Object> getKey(String code) {
        Mapper mapper = indexMapper.get(code);
        return mapper != null ? mapper.getKey() : null;
    }

    @Override
    public Map<String, Object> getValue(String code) {
        Mapper mapper = indexMapper.get(code);
        return mapper != null ? primaryKey.getValue(mapper.getPrimaryKeyCode()) : null;
    }

    @Override
    public void remove(Map<String, Object> doc) {
        indexMapper.remove(this.getCode(doc));
    }

    @Override
    public void save(Map<String, Object> doc) {
        Unique unique = this.getUnique(doc);

        Mapper mapper = new Mapper();
        mapper.setKey(unique.getUniqueData());
        mapper.setPrimaryKeyCode(primaryKey.getCode(doc));

        indexMapper.put(unique.getCode(), mapper);
    }

    @Override
    public Unique getUnique(Map<String, Object> doc) {
        return super.getUnique(doc, this.columns);
    }

    @Override
    public String getCode(Map<String, Object> data) {
        return primaryKey.getCode(data);
    }

    @Data
    private static class Mapper implements Serializable {

        private String primaryKeyCode;

        private Map<String, Object> key = Maps.newHashMap();

    }

}
