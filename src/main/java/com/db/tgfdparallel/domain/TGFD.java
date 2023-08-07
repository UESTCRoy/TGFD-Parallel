package com.db.tgfdparallel.domain;

import lombok.AllArgsConstructor;
import lombok.Data;

import java.io.Serializable;

@Data
@AllArgsConstructor
public class TGFD implements Serializable {
    private VF2PatternGraph pattern;
    private Pair delta;
    private DataDependency dependency;
    private Double tgfdSupport;
    private Double patternSupport;
    private int level;
    private int entitySize;

    @Override
    public String toString() {
        return "TGFD{" +
                "pattern=" + pattern.getPattern() +
                ", delta=" + delta +
                ", dependency=" + dependency +
                ", tgfdSupport=" + tgfdSupport +
                ", level=" + level +
                '}';
    }
}
