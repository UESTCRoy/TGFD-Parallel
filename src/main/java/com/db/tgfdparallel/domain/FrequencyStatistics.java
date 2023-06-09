package com.db.tgfdparallel.domain;

import lombok.AllArgsConstructor;
import lombok.Data;

import java.io.Serializable;

@Data
@AllArgsConstructor
public class FrequencyStatistics implements Serializable {
    private String type;
    private int frequency;
}
