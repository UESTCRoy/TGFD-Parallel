package com.db.tgfdparallel.domain;

import lombok.Data;
import lombok.NoArgsConstructor;

import java.util.HashSet;
import java.util.List;
import java.util.Set;

@Data
@NoArgsConstructor
public class AttributeDependency {
    Set<ConstantLiteral> lhs;
    ConstantLiteral rhs;
    private Delta delta;

    public AttributeDependency(List<ConstantLiteral> pathToRoot, ConstantLiteral rhs) {
        this.lhs = new HashSet<>(pathToRoot);
        this.rhs = rhs;
    }
}
