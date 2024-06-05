package com.jimmy.groot.engine.metadata;

import lombok.Data;

import java.io.Serializable;

@Data
public class Row implements Serializable {

    private String uniqueDataKey;

    private String partitionDataKey;

    private Long insertTime;

    private Long deleteTime;
}
