package com.db.tgfdparallel.domain;

import lombok.AllArgsConstructor;
import lombok.Data;

import java.io.Serializable;

@Data
@AllArgsConstructor
public class TGFD implements Serializable {
    // TODO: 给这个class加entitySize和k(level)
    private VF2PatternGraph pattern;
    private Delta delta;
    private DataDependency dependency;
    private Double tgfdSupport;
    private Double patternSupport;

    @Override
    public String toString() {
        return "TGFD{" +
                "pattern=" + pattern.getPattern() +
                ", delta=" + delta +
                ", dependency=" + dependency +
                ", tgfdSupport=" + tgfdSupport +
                '}';
    }
}
