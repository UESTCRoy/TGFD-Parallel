package com.db.tgfdparallel.domain;

import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;

import java.io.Serializable;

@Data
@AllArgsConstructor
@NoArgsConstructor
public class Change implements Serializable {
    private int id;
    private ChangeType changeType;

    public int getId() {
        return id;
    }

    public void setId(int id) {
        this.id = id;
    }

    public ChangeType getChangeType() {
        return changeType;
    }

    public void setChangeType(ChangeType changeType) {
        this.changeType = changeType;
    }

    //    private Set<String> types = new HashSet<>();
}
