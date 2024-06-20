package com.jimmy.groot.platform.core;

import lombok.Data;

import java.io.Serializable;

@Data
public class Event implements Serializable {

    private String type;

    private byte[] data;

}
