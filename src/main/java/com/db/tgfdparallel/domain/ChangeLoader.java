package com.db.tgfdparallel.domain;

import lombok.Data;

import java.util.HashMap;
import java.util.HashSet;
import java.util.List;

@Data
public class ChangeLoader {
    private List<Change> allChanges;
    private HashMap<Integer, HashSet<Change>> allGroupedChanges;
}
