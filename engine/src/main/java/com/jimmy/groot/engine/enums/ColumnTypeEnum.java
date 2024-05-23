package com.jimmy.groot.engine.enums;

import com.jimmy.groot.engine.base.Convert;
import com.jimmy.groot.engine.convert.DefaultConvert;
import lombok.AllArgsConstructor;
import lombok.Getter;

@Getter
@AllArgsConstructor
public enum ColumnTypeEnum {
    // 数值类型
    TINYINT("TINYINT", true, DefaultConvert.getInstance()),
    SMALLINT("SMALLINT", true, DefaultConvert.getInstance()),
    MEDIUMINT("MEDIUMINT", true, DefaultConvert.getInstance()),
    INT("INT", true, DefaultConvert.getInstance()),
    INTEGER("INTEGER", true, DefaultConvert.getInstance()),
    BIGINT("BIGINT", true, DefaultConvert.getInstance()),
    DECIMAL("DECIMAL", true, DefaultConvert.getInstance()),
    NUMERIC("NUMERIC", true, DefaultConvert.getInstance()),
    FLOAT("FLOAT", true, DefaultConvert.getInstance()),
    DOUBLE("DOUBLE", true, DefaultConvert.getInstance()),
    REAL("REAL", true, DefaultConvert.getInstance()),
    BIT("BIT", true, DefaultConvert.getInstance()),
    // 日期和时间类型
    DATE("DATE", true, DefaultConvert.getInstance()),
    TIME("TIME", true, DefaultConvert.getInstance()),
    DATETIME("DATETIME", true, DefaultConvert.getInstance()),
    TIMESTAMP("TIMESTAMP", true, DefaultConvert.getInstance()),
    YEAR("YEAR", true, DefaultConvert.getInstance()),
    // 字符串类型
    CHAR("CHAR", true, DefaultConvert.getInstance()),
    VARCHAR("VARCHAR", true, DefaultConvert.getInstance()),
    TINYTEXT("TINYTEXT", true, DefaultConvert.getInstance()),
    TEXT("TEXT", false, DefaultConvert.getInstance()),
    MEDIUMTEXT("MEDIUMTEXT", true, DefaultConvert.getInstance()),
    LONGTEXT("LONGTEXT", false, DefaultConvert.getInstance()),
    // 二进制类型
    BINARY("BINARY", true, DefaultConvert.getInstance()),
    VARBINARY("VARBINARY", true, DefaultConvert.getInstance()),
    TINYBLOB("TINYBLOB", true, DefaultConvert.getInstance()),
    BLOB("BLOB", false, DefaultConvert.getInstance()),
    MEDIUMBLOB("MEDIUMBLOB", false, DefaultConvert.getInstance()),
    LONGBLOB("LONGBLOB", false, DefaultConvert.getInstance()),
    // 枚举和集合类型
    ENUM("ENUM", true, DefaultConvert.getInstance()),
    SET("SET", true, DefaultConvert.getInstance()),
    // 空间数据类型
    GEOMETRY("GEOMETRY", true, DefaultConvert.getInstance()),
    POINT("POINT", true, DefaultConvert.getInstance()),
    LINESTRING("LINESTRING", true, DefaultConvert.getInstance()),
    POLYGON("POLYGON", true, DefaultConvert.getInstance()),
    MULTIPOINT("MULTIPOINT", true, DefaultConvert.getInstance()),
    MULTILINESTRING("MULTILINESTRING", true, DefaultConvert.getInstance()),
    MULTIPOLYGON("MULTIPOLYGON", true, DefaultConvert.getInstance()),
    GEOMETRYCOLLECTION("GEOMETRYCOLLECTION", true, DefaultConvert.getInstance()),
    // JSON 类型
    JSON("JSON", true, DefaultConvert.getInstance());

    private final String mysqlType;

    private final Boolean isStoreMemory;

    private final Convert<?> convert;

    public static ColumnTypeEnum queryByType(String type) {
        for (ColumnTypeEnum value : ColumnTypeEnum.values()) {
            if (value.mysqlType.equalsIgnoreCase(type)) {
                return value;
            }
        }

        return null;
    }
}
