package com.jimmy.groot.sql.enums;

import lombok.AllArgsConstructor;
import lombok.Getter;

@Getter
@AllArgsConstructor
public enum ColumnTypeEnum {
    // 数值类型
    TINYINT("TINYINT"),
    SMALLINT("SMALLINT"),
    MEDIUMINT("MEDIUMINT"),
    INT("INT"),
    INTEGER("INTEGER"),
    BIGINT("BIGINT"),
    DECIMAL("DECIMAL"),
    NUMERIC("NUMERIC"),
    FLOAT("FLOAT"),
    DOUBLE("DOUBLE"),
    REAL("REAL"),
    BIT("BIT"),
    // 日期和时间类型
    DATE("DATE"),
    TIME("TIME"),
    DATETIME("DATETIME"),
    TIMESTAMP("TIMESTAMP"),
    YEAR("YEAR"),
    // 字符串类型
    CHAR("CHAR"),
    VARCHAR("VARCHAR"),
    TINYTEXT("TINYTEXT"),
    TEXT("TEXT"),
    MEDIUMTEXT("MEDIUMTEXT"),
    LONGTEXT("LONGTEXT"),
    // 二进制类型
    BINARY("BINARY"),
    VARBINARY("VARBINARY"),
    TINYBLOB("TINYBLOB"),
    BLOB("BLOB"),
    MEDIUMBLOB("MEDIUMBLOB"),
    LONGBLOB("LONGBLOB"),
    // 枚举和集合类型
    ENUM("ENUM"),
    SET("SET"),
    // 空间数据类型
    GEOMETRY("GEOMETRY"),
    POINT("POINT"),
    LINESTRING("LINESTRING"),
    POLYGON("POLYGON"),
    MULTIPOINT("MULTIPOINT"),
    MULTILINESTRING("MULTILINESTRING"),
    MULTIPOLYGON("MULTIPOLYGON"),
    GEOMETRYCOLLECTION("GEOMETRYCOLLECTION"),
    // JSON 类型
    JSON("JSON");

    private final String mysqlType;

    public static ColumnTypeEnum queryByType(String type) {
        for (ColumnTypeEnum value : ColumnTypeEnum.values()) {
            if (value.mysqlType.equals(type)) {
                return value;
            }
        }

        return null;
    }
}
