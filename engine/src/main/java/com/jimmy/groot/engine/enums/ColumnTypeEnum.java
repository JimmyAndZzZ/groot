package com.jimmy.groot.engine.enums;

import com.jimmy.groot.engine.base.Convert;
import com.jimmy.groot.engine.convert.DefaultConvert;
import lombok.AllArgsConstructor;
import lombok.Getter;

@Getter
@AllArgsConstructor
public enum ColumnTypeEnum {
    // 数值类型
    TINYINT("TINYINT",  DefaultConvert.getInstance()),
    SMALLINT("SMALLINT",  DefaultConvert.getInstance()),
    MEDIUMINT("MEDIUMINT",  DefaultConvert.getInstance()),
    INT("INT",  DefaultConvert.getInstance()),
    INTEGER("INTEGER",  DefaultConvert.getInstance()),
    BIGINT("BIGINT",  DefaultConvert.getInstance()),
    DECIMAL("DECIMAL",  DefaultConvert.getInstance()),
    NUMERIC("NUMERIC",  DefaultConvert.getInstance()),
    FLOAT("FLOAT",  DefaultConvert.getInstance()),
    DOUBLE("DOUBLE",  DefaultConvert.getInstance()),
    REAL("REAL",  DefaultConvert.getInstance()),
    BIT("BIT",  DefaultConvert.getInstance()),
    // 日期和时间类型
    DATE("DATE",  DefaultConvert.getInstance()),
    TIME("TIME",  DefaultConvert.getInstance()),
    DATETIME("DATETIME",  DefaultConvert.getInstance()),
    TIMESTAMP("TIMESTAMP",  DefaultConvert.getInstance()),
    YEAR("YEAR",  DefaultConvert.getInstance()),
    // 字符串类型
    CHAR("CHAR",  DefaultConvert.getInstance()),
    VARCHAR("VARCHAR",  DefaultConvert.getInstance()),
    TINYTEXT("TINYTEXT",  DefaultConvert.getInstance()),
    TEXT("TEXT",  DefaultConvert.getInstance()),
    MEDIUMTEXT("MEDIUMTEXT",  DefaultConvert.getInstance()),
    LONGTEXT("LONGTEXT",  DefaultConvert.getInstance()),
    // 二进制类型
    BINARY("BINARY",  DefaultConvert.getInstance()),
    VARBINARY("VARBINARY",  DefaultConvert.getInstance()),
    TINYBLOB("TINYBLOB",  DefaultConvert.getInstance()),
    BLOB("BLOB",  DefaultConvert.getInstance()),
    MEDIUMBLOB("MEDIUMBLOB",  DefaultConvert.getInstance()),
    LONGBLOB("LONGBLOB",  DefaultConvert.getInstance()),
    // 枚举和集合类型
    ENUM("ENUM",  DefaultConvert.getInstance()),
    SET("SET",  DefaultConvert.getInstance()),
    // 空间数据类型
    GEOMETRY("GEOMETRY",  DefaultConvert.getInstance()),
    POINT("POINT",  DefaultConvert.getInstance()),
    LINESTRING("LINESTRING",  DefaultConvert.getInstance()),
    POLYGON("POLYGON",  DefaultConvert.getInstance()),
    MULTIPOINT("MULTIPOINT",  DefaultConvert.getInstance()),
    MULTILINESTRING("MULTILINESTRING",  DefaultConvert.getInstance()),
    MULTIPOLYGON("MULTIPOLYGON",  DefaultConvert.getInstance()),
    GEOMETRYCOLLECTION("GEOMETRYCOLLECTION",  DefaultConvert.getInstance()),
    // JSON 类型
    JSON("JSON",  DefaultConvert.getInstance());

    private final String mysqlType;

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
