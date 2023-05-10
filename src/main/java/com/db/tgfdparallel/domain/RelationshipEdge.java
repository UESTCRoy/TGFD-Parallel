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

        @Override
        public Vertex getTarget() {
                return (Vertex) super.getTarget();
        }

        @Override
        public Vertex getSource() {
                return (Vertex) super.getSource();
        }

        public RelationshipEdge copy() {
                // TODO: Do we need to copy the other two fields?
                return new RelationshipEdge(this.label);
        }
}
