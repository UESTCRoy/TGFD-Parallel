package com.db.tgfdparallel.domain;

import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.EqualsAndHashCode;

@Data
@EqualsAndHashCode
@AllArgsConstructor
public class Pair implements Comparable<Pair>{
    private int min;
    private int max;

    // TODO: 这里设置compareTo的理由
    @Override
    public int compareTo(Pair o) {
        if (this.min < o.min) {
            return -1;
        } else if (this.min > o.min) {
            return 1;
        } else {
            if (this.max < o.max) {
                return -1;
            } else if (this.max > o.max) {
                return 1;
            } else {
                return 0;
            }
        }
    }
}
