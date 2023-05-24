package com.db.tgfdparallel.domain;

import lombok.AllArgsConstructor;
import lombok.Data;

@Data
@AllArgsConstructor
public class TGFD {
    private VF2PatternGraph pattern;
    private Delta delta;
    private DataDependency dependency;
    private Double tgfdSupport;
    private Double patternSupport;
}
