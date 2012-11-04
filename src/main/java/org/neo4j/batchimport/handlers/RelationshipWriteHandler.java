package org.neo4j.batchimport.handlers;

import com.lmax.disruptor.EventHandler;
import org.neo4j.batchimport.Utils;
import org.neo4j.batchimport.collections.CompactLongRecord;
import org.neo4j.batchimport.structs.NodeStruct;
import org.neo4j.batchimport.structs.Relationship;
import org.neo4j.kernel.impl.nioneo.store.Record;

import java.io.IOException;

/**
* @author mh
* @since 27.10.12
*/
public class RelationshipWriteHandler implements EventHandler<NodeStruct> {
    private long counter;
    private final RelationshipWriter relationshipWriter;

    public RelationshipWriteHandler(RelationshipWriter relationshipWriter) {
        this.relationshipWriter = relationshipWriter;
    }

    @Override
    public void onEvent(NodeStruct event, long nodeId, boolean endOfBatch) throws Exception {
        if (Record.NO_NEXT_RELATIONSHIP.is(event.nextRel)) return;
        relationshipWriter.start(event.maxRelationshipId);

        int count = event.relationshipCount;
        long followingNextRelationshipId = Record.NO_NEXT_RELATIONSHIP.intValue();
        if (event.relationshipsToUpdate !=null) {
            long value = event.relationshipsToUpdate.firstPositive();
            if (value == -1) value = event.relationshipsToUpdate.firstNegative();
            if (value != -1) followingNextRelationshipId = value;
        }

        long prevId = Record.NO_PREV_RELATIONSHIP.intValue();
        for (int i = 0; i < count; i++) {
            long nextId = i+1 < count ? event.getRelationship(i+1).id : followingNextRelationshipId;
            Relationship relationship = event.getRelationship(i);
            relationshipWriter.create(nodeId,event, relationship, prevId, nextId);
            prevId = relationship.id;
            counter++;
        }

        if (event.relationshipsToUpdate!=null) {

            followingNextRelationshipId = event.relationshipsToUpdate.firstNegative();

            prevId = createUpdateRecords(event.relationshipsToUpdate.getOutgoing(), prevId, followingNextRelationshipId,true);

            followingNextRelationshipId = Record.NO_NEXT_RELATIONSHIP.intValue();

            createUpdateRecords(event.relationshipsToUpdate.getIncoming(), prevId, followingNextRelationshipId, false);
        }
            if (endOfBatch) relationshipWriter.flush();
    }

    private long createUpdateRecords(long[] relIds, long prevId, long followingNextRelationshipId, boolean outgoing) throws IOException {
        if (relIds==null || relIds.length==0) return prevId;
        int count = relIds.length;
        for (int i = 0; i < count; i++) {
            long nextId = i+1 < count ? relIds[i + 1] : followingNextRelationshipId;
            relationshipWriter.update(relIds[i], outgoing, prevId, nextId);
            prevId = relIds[i];
            counter++;
        }
        return prevId;
    }

    @Override
    public String toString() {
        return "rel-record-writer  " + counter + " "+relationshipWriter;
    }
    public void close() {
        try {
            relationshipWriter.close();
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
    }
}
