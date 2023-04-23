package com.db.tgfdparallel.domain;

import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.EqualsAndHashCode;
import lombok.NoArgsConstructor;
import org.jgrapht.graph.DefaultEdge;

@Data
@EqualsAndHashCode(callSuper = true)
@NoArgsConstructor
@AllArgsConstructor
public class RelationshipEdge extends DefaultEdge {
        private String label;
}
