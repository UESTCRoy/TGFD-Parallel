package com.db.tgfdparallel.config;

import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;

import java.util.concurrent.atomic.AtomicBoolean;

@Configuration
public class AtomicBooleanConfig {
    @Bean
    public AtomicBoolean workersStatusChecker() {
        return new AtomicBoolean(true);
    }
}