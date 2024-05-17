package com.jimmy.groot.engine.base;

import com.jimmy.groot.sql.enums.IndexTypeEnum;

import java.io.Serializable;
import java.util.Set;

public interface Index extends Serializable {

    IndexTypeEnum type();

    Set<String> getColumns();
}
