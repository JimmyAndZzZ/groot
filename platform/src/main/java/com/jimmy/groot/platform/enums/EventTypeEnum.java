package com.jimmy.groot.platform.enums;

import lombok.AllArgsConstructor;
import lombok.Getter;

@Getter
@AllArgsConstructor
public enum EventTypeEnum {

    HEARTBEAT("Heartbeat", "心跳检测"),
    REGISTER("Register", "引擎注册");

    private final String code;

    private final String message;

    public static EventTypeEnum queryByCode(String code) {
        for (EventTypeEnum value : EventTypeEnum.values()) {
            if (value.code.equalsIgnoreCase(code)) {
                return value;
            }
        }

        return null;
    }
}
