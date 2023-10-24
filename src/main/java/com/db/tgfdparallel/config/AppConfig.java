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

//    @Value("${superStep")
//    private int superStep;

    @Value("#{'${workers}'.split(',')}")
    private List<String> workers;

    @Value("#{'${splitGraphPath}'.split(',')}")
    private List<String> splitGraphPath;

    @Value("${changeFilePath}")
    private String changeFilePath;

    @Value("${dataPath}")
    private String dataPath;

    @Value("#{'${allDataPath}'.split(',')}")
    private List<String> allDataPath;

    @Value("${timestamp}")
    private int timestamp;

    @Value("${k}")
    private int k;

    @Value("${patternTheta}")
    private double patternTheta;

    @Value("${tgfdTheta}")
    private double tgfdTheta;

    @Value("${activeAttribute}")
    private int activeAttributes;

    @Value("${frequentEdges}")
    private int frequentEdges;

    @Value("${mode}")
    private String mode;

    @Value("${bucketName}")
    private String bucketName;

    @Value("${HDFSName}")
    private String HDFSName;

    @Value("${HDFSAddress}")
    private String HDFSAddress;

    @Value("${HDFSPath}")
    private String HDFSPath;

    @Value("${vpcNumber}")
    private int vpcNumber;

    @Value("${instanceID}")
    private String instanceID;
}
