package com.db.tgfdparallel.domain;

import lombok.AllArgsConstructor;
import lombok.Data;

import java.io.Serializable;
import java.time.Duration;
import java.time.Period;

@Data
@AllArgsConstructor
public class Delta implements Serializable {
    private Period min;
    private Period max;
//    private Duration granularity;
}
