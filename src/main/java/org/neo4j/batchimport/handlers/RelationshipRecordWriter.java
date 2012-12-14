package org.neo4j.batchimport.handlers;

import org.neo4j.batchimport.structs.NodeStruct;
import org.neo4j.batchimport.structs.Relationship;
import org.neo4j.kernel.impl.nioneo.store.RelationshipRecord;
import org.neo4j.kernel.impl.nioneo.store.RelationshipStore;

import java.io.IOException;

/**
* @author mh
* @since 27.10.12
*/
public class RelationshipRecordWriter implements RelationshipWriter {
    private final RelationshipStore relationshipStore;

    volatile long created, updated;
    
    public RelationshipRecordWriter(RelationshipStore relationshipStore) {
        this.relationshipStore = relationshipStore;
    }

    @Override
    public void create(long nodeId, NodeStruct event, Relationship relationship, long prevId, long nextId) {
        updateRecord(createRecord(nodeId, relationship,prevId,nextId));
        created++;
    }

    @Override
    public boolean update(long relId, boolean outgoing, long prevId, long nextId) {
        RelationshipRecord record = relationshipStore.getLightRel(relId);
        if (record==null) return false;
        if (outgoing) {
            record.setFirstPrevRel(prevId);
            record.setFirstNextRel(nextId);
        } else {
            record.setSecondPrevRel(prevId);
            record.setSecondNextRel(nextId);
        }
        updateRecord(record);
        updated++;
        return true;
    }

    private void updateRecord(RelationshipRecord record) {
        relationshipStore.updateRecord(record);
    }

    @Override
    public void flush() {
        relationshipStore.flushAll();
    }

    @Override
    public void start(long maxRelationshipId) {
        if (relationshipStore.getHighId() <= maxRelationshipId) relationshipStore.setHighId(maxRelationshipId +1);
    }

    @Override
    public void close() throws IOException {
        flush();
    }

    private RelationshipRecord createRecord(long from, Relationship relationship, long prevId, long nextId) {
        long id = relationship.id;
        RelationshipRecord relRecord = relationship.outgoing() ?
                    new RelationshipRecord( id, from, relationship.other(), relationship.type ) :
                    new RelationshipRecord( id, relationship.other(), from,  relationship.type );
        relRecord.setInUse(true);
        relRecord.setCreated();
        if (relationship.outgoing()) {
            relRecord.setFirstPrevRel(prevId);
            relRecord.setFirstNextRel(nextId);
        } else {
            relRecord.setSecondPrevRel(prevId);
            relRecord.setSecondNextRel(nextId);
        }
        relRecord.setNextProp(relationship.firstPropertyId);
        return relRecord;
    }

    @Override
    public String toString() {
        return String.format("RelationshipRecordWriter created %d updated %d %n",created,updated);
    }
}
