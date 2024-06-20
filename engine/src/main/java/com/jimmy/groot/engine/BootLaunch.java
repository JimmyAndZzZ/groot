package com.jimmy.groot.engine;

import com.jimmy.groot.engine.core.ConfigLoad;
import com.jimmy.groot.engine.netty.Client;
import com.jimmy.groot.platform.constant.ConfigConstant;

public class BootLaunch {

    public static void main(String[] args) {
        Client.build(ConfigLoad.get(ConfigConstant.Client.SERVER_ADDRESS)).connect();
    }
}
