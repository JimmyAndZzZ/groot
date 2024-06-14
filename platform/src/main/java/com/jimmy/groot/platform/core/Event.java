package com.jimmy.groot.platform.core;

import com.jimmy.groot.platform.enums.EventTypeEnum;
import lombok.Data;

import java.io.Serializable;

@Data
public class Event implements Serializable {

    private String type;

    private String message;

    public Event(EventTypeEnum type, String message) {
        this.message = message;
        this.type = type.getCode();
    }

    public Event() {

    }
}
