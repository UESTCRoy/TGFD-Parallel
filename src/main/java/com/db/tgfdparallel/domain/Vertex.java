package com.db.tgfdparallel.domain;


import lombok.Data;
import lombok.EqualsAndHashCode;
import lombok.NoArgsConstructor;

import java.io.Serializable;
import java.util.HashSet;
import java.util.Objects;
import java.util.Set;

@Data
@NoArgsConstructor
@EqualsAndHashCode(onlyExplicitlyIncluded = true)
public class Vertex implements Serializable {
    @EqualsAndHashCode.Include
    private String uri;
    //    @EqualsAndHashCode.Include
    private String type;
    private Set<Attribute> attributes;
    private boolean isMarked;
//    private Set<Integer> jobletID;

    public Vertex(String uri, String type) {
        this.uri = uri;
        this.type = type;
        this.attributes = new HashSet<>();
    }

    public Vertex(String uri, String type, Set<Attribute> attributes) {
        this.uri = uri;
        this.type = type;
        this.attributes = attributes;
    }

    public Vertex(String type) {
        this.uri = type;
        this.type = type;
        this.attributes = new HashSet<>();
    }

    public boolean isMapped(Vertex other) {
        if (other == null) {
            return false;
        }

        if (!this.getType().equals(other.getType())) {
            return false;
        }

        if (!other.getAttributes().isEmpty()) {
            if (!Objects.equals(other.getAttributes(), this.getAttributes())) {
                return false;
            }
        }

        return true;
    }

//    public boolean isMapped(Vertex other) {
//        if (other == null) {
//            return false;
//        }
//
//        if (!this.uri.equals(other.uri) || !this.types.equals(other.types)) {
//            return false;
//        }
//
//        if (this.attributes.size() != other.attributes.size()) {
//            return false;
//        }
//
//        for (Attribute attr : this.attributes) {
//            boolean found = false;
//            for (Attribute otherAttr : other.attributes) {
//                if (attr.getAttrName().equals(otherAttr.getAttrName()) && attr.getAttrValue().equals(otherAttr.getAttrValue())) {
//                    found = true;
//                    break;
//                }
//            }
//            if (!found) {
//                return false;
//            }
//        }
//
//        return true;
//    }

}
