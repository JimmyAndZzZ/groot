package com.jimmy.groot.engine.base;

import java.util.Date;

public interface Convert<T> {

    T convert(Object o);

    String toString(T o);

}
