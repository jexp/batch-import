package org.neo4j.batchimport.importer;

import org.neo4j.graphdb.RelationshipType;

public class RelType implements RelationshipType {
    String name;

    public RelType update(Object value) {
        this.name = value.toString();
        return this;
    }

    public String name() {
        return name;
    }
}
