package org.neo4j.batchimport.structs;

import org.neo4j.kernel.impl.nioneo.store.Record;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

/**
* @author mh
* @since 27.10.12
*/
public class NodeStruct extends PropertyHolder {
    public volatile long firstRel = Record.NO_NEXT_RELATIONSHIP.intValue();

    public volatile long prevId;

    private final Relationship[] relationships;
    public final List<Relationship> moreRelationships = new ArrayList<Relationship>();
    public volatile int relationshipCount;

    public volatile long lastPropertyId;

    private static int avgRelCount;
    private static int relPropertyCount;

    public static void classInit(int avgRelCount, int relPropertyCount) {
        NodeStruct.avgRelCount = avgRelCount;
        NodeStruct.relPropertyCount = relPropertyCount;
    }

    public NodeStruct(int propertyCount) {
        super(propertyCount);
        this.relationships=new Relationship[NodeStruct.avgRelCount];
        for (int i = 0; isRelInArray(i); i++) {
            relationships[i]=new Relationship(NodeStruct.relPropertyCount);
        }
    }

    public NodeStruct init() {
        super.init();
        firstRel = Record.NO_NEXT_RELATIONSHIP.intValue();
        prevId = Record.NO_PREV_RELATIONSHIP.intValue();
        clearRelationshipInfo();
        return this;
    }

    public void clearRelationshipInfo() {
        if (!isRelInArray(relationshipCount)) moreRelationships.clear();
        relationshipCount=0;
    }

    public Relationship addRel(long other, boolean outgoing, int type) {
        if (isRelInArray(relationshipCount++)) {
            return relationships[relationshipCount-1].init(other,outgoing,type);
        }
        Relationship rel = new Relationship(relPropertyCount).init(other, outgoing, type);
        moreRelationships.add(rel);
        return rel;
    }

    public Relationship getRelationship(int i) {
        if (isRelInArray(i)) return relationships[i];
        return moreRelationships.get(i-avgRelCount);
    }

    private boolean isRelInArray(int i) {
        return i<avgRelCount;
    }

    @Override
    public String toString() {
        StringBuilder rels=new StringBuilder();
        for (int i=0;i<relationshipCount;i++) {
            rels.append(getRelationship(i)).append("\n");
        }
        return "NodeStruct{" +
                " firstRel=" + firstRel +
                ", relationshipCount=" + relationshipCount +
                ", lastPropertyId=" + lastPropertyId +
                " relationships=\n" + rels +
                '}';
    }
}

