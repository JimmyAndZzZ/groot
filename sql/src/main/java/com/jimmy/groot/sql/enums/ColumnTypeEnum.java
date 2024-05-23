package com.jimmy.groot.sql.enums;

import lombok.AllArgsConstructor;
import lombok.Getter;

@Getter
@AllArgsConstructor
public enum ColumnTypeEnum {
    // 数值类型
    TINYINT("TINYINT", true),
    SMALLINT("SMALLINT", true),
    MEDIUMINT("MEDIUMINT", true),
    INT("INT", true),
    INTEGER("INTEGER", true),
    BIGINT("BIGINT", true),
    DECIMAL("DECIMAL", true),
    NUMERIC("NUMERIC", true),
    FLOAT("FLOAT", true),
    DOUBLE("DOUBLE", true),
    REAL("REAL", true),
    BIT("BIT", true),
    // 日期和时间类型
    DATE("DATE", true),
    TIME("TIME", true),
    DATETIME("DATETIME", true),
    TIMESTAMP("TIMESTAMP", true),
    YEAR("YEAR", true),
    // 字符串类型
    CHAR("CHAR", true),
    VARCHAR("VARCHAR", true),
    TINYTEXT("TINYTEXT", true),
    TEXT("TEXT", false),
    MEDIUMTEXT("MEDIUMTEXT", true),
    LONGTEXT("LONGTEXT", false),
    // 二进制类型
    BINARY("BINARY", true),
    VARBINARY("VARBINARY", true),
    TINYBLOB("TINYBLOB", true),
    BLOB("BLOB", false),
    MEDIUMBLOB("MEDIUMBLOB", false),
    LONGBLOB("LONGBLOB", false),
    // 枚举和集合类型
    ENUM("ENUM", true),
    SET("SET", true),
    // 空间数据类型
    GEOMETRY("GEOMETRY", true),
    POINT("POINT", true),
    LINESTRING("LINESTRING", true),
    POLYGON("POLYGON", true),
    MULTIPOINT("MULTIPOINT", true),
    MULTILINESTRING("MULTILINESTRING", true),
    MULTIPOLYGON("MULTIPOLYGON", true),
    GEOMETRYCOLLECTION("GEOMETRYCOLLECTION", true),
    // JSON 类型
    JSON("JSON", true);

    private final String mysqlType;

    private final Boolean isStoreMemory;

    public static ColumnTypeEnum queryByType(String type) {
        for (ColumnTypeEnum value : ColumnTypeEnum.values()) {
            if (value.mysqlType.equals(type)) {
                return value;
            }
        }

        return null;
    }
}
