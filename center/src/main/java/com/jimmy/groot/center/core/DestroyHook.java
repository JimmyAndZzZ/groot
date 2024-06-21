package com.jimmy.groot.center.core;

import com.google.common.collect.Lists;
import lombok.extern.slf4j.Slf4j;

import java.util.List;

@Slf4j
public class DestroyHook {

    private static final List<Runnable> HOOKS = Lists.newArrayList();

    static {
        Runtime.getRuntime().addShutdownHook(new Thread(() -> {
            log.info("Shutdown hook triggered.");
            for (Runnable hook : HOOKS) {
                hook.run();
            }
        }));
    }

    public static void registerHook(Runnable runnable) {
        HOOKS.add(runnable);
    }
}
