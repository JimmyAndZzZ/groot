package com.jimmy.groot.engine.process;

import com.jimmy.groot.platform.base.Message;
import com.jimmy.groot.platform.core.message.Heartbeat;

public class HeartbeatProcess extends CallbackProcess<Heartbeat>{

    @Override
    public Message callback(Heartbeat heartbeat) {
        return heartbeat;
    }
}
