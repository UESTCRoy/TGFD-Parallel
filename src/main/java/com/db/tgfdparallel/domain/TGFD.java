package com.db.tgfdparallel.domain;

import lombok.AllArgsConstructor;
import lombok.Data;

import java.io.Serializable;

@Data
@AllArgsConstructor
public class TGFD implements Serializable {
    private VF2PatternGraph pattern;
    private Delta delta;
    private DataDependency dependency;
    private Double tgfdSupport;
    private Double patternSupport;
}
