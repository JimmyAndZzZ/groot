package com.jimmy.groot.platform.core.center;

import com.jimmy.groot.platform.enums.EngineStatusEnum;
import lombok.Data;

import java.io.Serializable;

@Data
public class Engine implements Serializable {

    private String id;

    private String ipAddress;

    private EngineStatusEnum engineStatus;

}
