package com.jimmy.groot.engine.core;

import com.google.common.collect.Lists;
import lombok.Data;

import java.io.Serializable;
import java.util.List;

@Data
public class Partition implements Serializable {

    private List<Fragment> fragments = Lists.newArrayList();
}
