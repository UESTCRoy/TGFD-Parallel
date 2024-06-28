package com.db.tgfdparallel.domain;

import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.EqualsAndHashCode;

import java.io.Serializable;

@Data
@AllArgsConstructor
@EqualsAndHashCode(onlyExplicitlyIncluded = true)
public class TGFD implements Serializable {
    @EqualsAndHashCode.Include
    private Pair delta;
    @EqualsAndHashCode.Include
    private DataDependency dependency;
    private Double tgfdSupport;
    private int level;
    private int dependencyKey;
    private int numberOfPairs;

    public TGFD(Pair delta, DataDependency dependency, Double tgfdSupport, int level) {
        this.delta = delta;
        this.dependency = dependency;
        this.tgfdSupport = tgfdSupport;
        this.level = level;
    }

    @Override
    public String toString() {
        return "TGFD{" +
                "dependency=" + dependency +
                ", delta=" + delta +
                ", tgfdSupport=" + tgfdSupport +
                ", level=" + level +
                '}';
    }
}
