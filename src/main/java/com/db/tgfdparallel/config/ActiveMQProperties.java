package com.db.tgfdparallel.config;

import lombok.Data;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.boot.context.properties.ConfigurationProperties;
import org.springframework.context.annotation.Configuration;

@Configuration
@ConfigurationProperties(prefix = "activemq")
@Data
public class ActiveMQProperties {
    @Value("${activemq.broker-url}")
    private String brokerUrl;
    @Value("${activemq.user}")
    private String user;
    @Value("${activemq.password}")
    private String password;
}
