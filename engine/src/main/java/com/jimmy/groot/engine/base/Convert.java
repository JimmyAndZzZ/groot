package com.jimmy.groot.engine.base;

public interface Convert<T> {

    T convert(Object o);

    String toString(T o);

}
