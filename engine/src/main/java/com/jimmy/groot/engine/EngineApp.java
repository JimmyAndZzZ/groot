package com.jimmy.groot.engine;

import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.boot.autoconfigure.jdbc.DataSourceAutoConfiguration;

@SpringBootApplication(exclude = {DataSourceAutoConfiguration.class})
public class EngineApp {

    public static void main(String[] args) {
        SpringApplication.run(EngineApp.class, args);
    }
}
