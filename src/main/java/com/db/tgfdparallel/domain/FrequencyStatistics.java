package com.db.tgfdparallel.domain;

import lombok.AllArgsConstructor;
import lombok.Data;

@Data
@AllArgsConstructor
public class FrequencyStatistics {
    private String type;
    private int frequency;
}
