package com.db.tgfdparallel.config;

import lombok.Data;
import org.springframework.boot.context.properties.ConfigurationProperties;
import org.springframework.stereotype.Component;

@Component
@ConfigurationProperties(prefix = "activemq")
@Data
public class ActiveMQProperties {
    private String brokerUrl;
    private String user;
    private String password;
}
