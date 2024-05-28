package com.jimmy.groot.engine.convert;

import com.jimmy.groot.engine.base.Convert;

import java.util.Date;

public class DefaultConvert implements Convert<Object> {

    private static class SingletonHolder {
        private static final DefaultConvert INSTANCE = new DefaultConvert();
    }

    public static DefaultConvert getInstance() {
        return DefaultConvert.SingletonHolder.INSTANCE;
    }

    private DefaultConvert() {

    }

    @Override
    public Object convert(Object o) {
        return o;
    }

    @Override
    public String toString(Object o) {
        return o == null ? null : o.toString();
    }
}
