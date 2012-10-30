package org.neo4j.batchimport.structs;

import org.neo4j.kernel.impl.nioneo.store.Record;

/**
* @author mh
* @since 27.10.12
*/
public class NodeStruct extends PropertyHolder {
    //long p1,p2,p3,p4,p5,p6,p7;
    public volatile long nextRel = Record.NO_NEXT_RELATIONSHIP.intValue();
    //long o1,o2,o3,o4,o5,o6,o7;

    public final Relationship[] relationships;
    public volatile int relationshipCount;

    public volatile long lastPropertyId;
    public volatile long maxRelationshipId;
    public volatile long[] outgoingRelationshipsToUpdate;
    public volatile long[] incomingRelationshipsToUpdate;

    public NodeStruct(int propertyCount, int relCount, int relPropertyCount) {
        super(propertyCount);
        this.relationships=new Relationship[relCount];
        for (int i = 0; i < relCount; i++) {
            relationships[i]=new Relationship(relPropertyCount);
        }
    }

    public NodeStruct init() {
        super.init();
        relationshipCount=0;
        nextRel = Record.NO_NEXT_RELATIONSHIP.intValue();
        return this;
    }
    public Relationship addRel(long other, boolean outgoing, int type) {
        return relationships[relationshipCount++].init(other,outgoing,type);
    }
}
