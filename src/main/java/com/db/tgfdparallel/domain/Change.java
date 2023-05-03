package com.db.tgfdparallel.domain;

import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;

@Data
@AllArgsConstructor
@NoArgsConstructor
public class Change {
    private int id;
    private ChangeType changeType;
//    private Set<String> types = new HashSet<>();
}
