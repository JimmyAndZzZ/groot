package com.jimmy.groot.platform.core.message;

import com.jimmy.groot.platform.base.Message;
import com.jimmy.groot.platform.enums.EventTypeEnum;
import lombok.Data;

@Data
public class Register implements Message {

    private String id;

    private String ipAddress;

    @Override
    public EventTypeEnum type() {
        return EventTypeEnum.REGISTER;
    }
}
