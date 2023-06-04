package com.db.tgfdparallel.domain;


import lombok.Data;
import lombok.EqualsAndHashCode;
import lombok.NoArgsConstructor;

import java.util.HashSet;
import java.util.Set;

@Data
@NoArgsConstructor
@EqualsAndHashCode(onlyExplicitlyIncluded = true)
public class Vertex {
    @EqualsAndHashCode.Include
    private String uri;
    @EqualsAndHashCode.Include
    private String types;
    private Set<Attribute> attributes;
    private boolean isMarked;
//    private Set<Integer> jobletID;

    public Vertex(String uri, String types) {
        this.uri = uri;
        this.types = types;
        attributes = new HashSet<>();
    }

    public Vertex(String types) {
        this.types = types;
    }

    // TODO: Can we just compare VertexURI?
    public boolean isMapped(Vertex other) {
        if (other == null) {
            return false;
        }

        if (!this.uri.equals(other.uri) || !this.types.equals(other.types)) {
            return false;
        }

        if (this.attributes.size() != other.attributes.size()) {
            return false;
        }

        for (Attribute attr : this.attributes) {
            boolean found = false;
            for (Attribute otherAttr : other.attributes) {
                if (attr.getAttrName().equals(otherAttr.getAttrName()) && attr.getAttrValue().equals(otherAttr.getAttrValue())) {
                    found = true;
                    break;
                }
            }
            if (!found) {
                return false;
            }
        }

        return true;
    }

}
