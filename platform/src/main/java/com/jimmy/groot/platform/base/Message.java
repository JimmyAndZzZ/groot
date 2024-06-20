package com.jimmy.groot.platform.base;


import com.jimmy.groot.platform.enums.EventTypeEnum;

import java.io.Serializable;

public interface Message extends Serializable {

    EventTypeEnum type();
}
