package org.neo4j.batchimport;

import org.hamcrest.BaseMatcher;
import org.hamcrest.Description;
import org.neo4j.graphdb.DynamicRelationshipType;
import org.neo4j.graphdb.RelationshipType;

class RelationshipMatcher extends BaseMatcher<RelationshipType> {
    public RelationshipType type;
    private Object other;

    RelationshipMatcher(RelationshipType type) {
        this.type = type;
    }
    RelationshipMatcher(String name) {
        this(DynamicRelationshipType.withName(name));
    }

    public boolean matches(Object other) {
        this.other = other;
        return ((RelationshipType)other).name().equals(type.name());
    }

    @Override
    public void describeTo(Description description) {
        description.appendText("Expected "+ type +" but got "+other);
    }
}
