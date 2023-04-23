package com.db.tgfdparallel.config;

import lombok.Data;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.stereotype.Component;

import java.util.List;

@Component
@Data
public class AppConfig {
    @Value("${nodeName}")
    private String nodeName;

    @Value("${dataset}")
    private String dataset;

    @Value("${superStep")
    private int superStep;

    @Value("${workers}")
    private String[] workers;

    @Value("#{'${splitGraphPath}'.split(',')}")
    private List<String> splitGraphPath;

    @Value("${changeFilePath}")
    private String changeFilePath;

    @Value("${dataPath}")
    private String dataPath;

    @Value("${timestamp}")
    private int timestamp;

    @Value("${k}")
    private int k;

    @Value("${theta}")
    private double theta;

    @Value("${activeAttribute}")
    private int activeAttributes;

    @Value("${frequentEdges}")
    private int frequentEdges;


}
