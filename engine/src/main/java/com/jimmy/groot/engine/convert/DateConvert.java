package com.jimmy.groot.engine.convert;

import cn.hutool.core.date.DatePattern;
import cn.hutool.core.date.DateUtil;
import com.jimmy.groot.engine.base.Convert;

import java.util.Date;

public class DateConvert implements Convert<Date> {

    @Override
    public Date convert(Object o) {
        return o != null ? DateUtil.parse(o.toString()) : null;
    }

    @Override
    public String toString(Date o) {
        return o != null ? DateUtil.format(o, DatePattern.NORM_DATETIME_MS_PATTERN) : null;
    }
}
