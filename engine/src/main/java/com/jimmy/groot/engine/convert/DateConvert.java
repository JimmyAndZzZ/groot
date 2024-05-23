package com.jimmy.groot.engine.convert;

import cn.hutool.core.date.DateUtil;
import com.jimmy.groot.engine.base.Convert;

import java.util.Date;

public class DateConvert implements Convert<Date> {

    @Override
    public Date convert(Object o) {
        return DateUtil.parse(o.toString());
    }
}
