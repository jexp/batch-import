package org.neo4j.batchimport.handlers;

import com.lmax.disruptor.EventHandler;
import org.neo4j.batchimport.Utils;
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
        long followingNextRelationshipId =
                event.outgoingRelationshipsToUpdate!=null ? event.outgoingRelationshipsToUpdate[0] :
                event.incomingRelationshipsToUpdate!=null ? event.incomingRelationshipsToUpdate[0] :
                                                            Record.NO_NEXT_RELATIONSHIP.intValue();

        long prevId = Record.NO_PREV_RELATIONSHIP.intValue();
        for (int i = 0; i < count; i++) {
            long nextId = i+1 < count ? event.relationships[i + 1].id : followingNextRelationshipId;
            Relationship relationship = event.relationships[i];
            relationshipWriter.create(nodeId,event, relationship, prevId, nextId);
            prevId = relationship.id;
            counter++;
        }

        followingNextRelationshipId =
                event.incomingRelationshipsToUpdate!=null ? event.incomingRelationshipsToUpdate[0] :
                                                            Record.NO_NEXT_RELATIONSHIP.intValue();

        prevId = createUpdateRecords(event.outgoingRelationshipsToUpdate, prevId, followingNextRelationshipId,true);

        followingNextRelationshipId = Record.NO_NEXT_RELATIONSHIP.intValue();

        createUpdateRecords(event.incomingRelationshipsToUpdate, prevId, followingNextRelationshipId, false);

       if (endOfBatch) relationshipWriter.flush();
    }

    private long createUpdateRecords(int[] relIds, long prevId, long followingNextRelationshipId, boolean outgoing) throws IOException {
        if (relIds==null) return prevId;
        int count = Utils.size(relIds);
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
