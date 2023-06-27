package com.db.tgfdparallel.domain;

import lombok.Data;
import lombok.EqualsAndHashCode;

import java.util.HashSet;
import java.util.List;
import java.util.Set;

@Data
@EqualsAndHashCode(onlyExplicitlyIncluded = true)
public class AttributeDependency {
    @EqualsAndHashCode.Include
    Set<ConstantLiteral> lhs;
    @EqualsAndHashCode.Include
    ConstantLiteral rhs;
    private Delta delta;

    public AttributeDependency(List<ConstantLiteral> pathToRoot, ConstantLiteral rhs) {
        this.lhs = new HashSet<>(pathToRoot);
        this.rhs = rhs;
    }

    public AttributeDependency() {
        this.lhs = new HashSet<>();
    }
}
