package org.neo4j.batchimport.structs;

import org.neo4j.batchimport.collections.CompactLongRecord;
import org.neo4j.batchimport.collections.CompactLongRecord2;
import org.neo4j.kernel.impl.nioneo.store.Record;

import java.util.ArrayList;
import java.util.List;

/**
* @author mh
* @since 27.10.12
*/
public class NodeStruct extends PropertyHolder {
    //long p1,p2,p3,p4,p5,p6,p7;
    public volatile long nextRel = Record.NO_NEXT_RELATIONSHIP.intValue();
    //long o1,o2,o3,o4,o5,o6,o7;

    private final Relationship[] relationships;
    public final List<Relationship> moreRelationships = new ArrayList<Relationship>();
    public volatile int relationshipCount;

    public volatile long lastPropertyId;

    private static int avgRelCount;
    private static int relPropertyCount;
    public volatile long prevId;
    public volatile CompactLongRecord2 relationshipsToUpdate;

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
        if (!isRelInArray(relationshipCount)) moreRelationships.clear();
        relationshipCount=0;
        nextRel = Record.NO_NEXT_RELATIONSHIP.intValue();
        return this;
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
}
