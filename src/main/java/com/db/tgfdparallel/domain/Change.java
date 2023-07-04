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
    //    private Set<String> types = new HashSet<>();
}
