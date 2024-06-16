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
    private int entitySize;

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
