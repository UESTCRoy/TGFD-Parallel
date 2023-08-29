package com.db.tgfdparallel.config;

import lombok.Data;
import org.springframework.stereotype.Component;

@Component
@Data
public class HDFSProperties {
    private String HDFSName;
    private String HDFSAddress;
    private String HDFSPath;
}
