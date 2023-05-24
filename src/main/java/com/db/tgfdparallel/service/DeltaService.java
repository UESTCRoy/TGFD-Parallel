package com.db.tgfdparallel.service;

import com.db.tgfdparallel.config.AppConfig;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.stereotype.Service;

@Service
public class DeltaService {
    private static final Logger logger = LoggerFactory.getLogger(DeltaService.class);
    private final AppConfig config;

    public DeltaService(AppConfig config) {
        this.config = config;
    }


}
