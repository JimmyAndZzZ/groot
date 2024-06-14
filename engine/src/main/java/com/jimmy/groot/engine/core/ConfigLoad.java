package com.jimmy.groot.engine.core;

import java.io.BufferedReader;
import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStreamReader;
import java.util.Properties;

public class ConfigLoad {

    private Properties properties;

    private static ConfigLoad configLoad;

    private ConfigLoad() {
        loadProperties();
    }

    public static String get(String key) {
        if (configLoad == null) {
            synchronized (ConfigLoad.class) {
                if (configLoad == null) {
                    configLoad = new ConfigLoad();
                }
            }
        }
        return configLoad.properties.getProperty(key);
    }

    /**
     * 读取配置
     *
     * @return
     */
    private void loadProperties() {
        try (FileInputStream fileInputStream = new FileInputStream("engine.properties");
             InputStreamReader inputStreamReader = new InputStreamReader(fileInputStream);
             BufferedReader bfReader = new BufferedReader(inputStreamReader)) {

            this.properties = new Properties();
            properties.load(bfReader);
        } catch (IOException e) {
            e.printStackTrace();
        }
    }
}
