package org.neo4j.batchimport.handlers;

import com.lmax.disruptor.EventHandler;
import org.neo4j.batchimport.collections.CompactLongRecord;
import org.neo4j.batchimport.collections.CompactLongRecord2;
import org.neo4j.batchimport.collections.ConcurrentLongReverseRelationshipMap;
import org.neo4j.batchimport.collections.ReverseRelationshipMap;
import org.neo4j.batchimport.structs.NodeStruct;
import org.neo4j.batchimport.structs.Relationship;
import org.neo4j.kernel.impl.nioneo.store.Record;

import java.io.File;
import java.io.IOException;

/**
* @author mh
* @since 27.10.12
*/
public class RelationshipUpdateHandler implements EventHandler<NodeStruct> {
    private long counter;
    private final RelationshipFileUpdater relationshipFileUpdater;

    public RelationshipUpdateHandler(final File relFile) {
        this.relationshipFileUpdater = new RelationshipFileUpdater(relFile);
    }

    @Override
    public void onEvent(NodeStruct event, long nodeId, boolean endOfBatch) throws Exception {
        if (event.relationshipsToUpdate ==null) return;
        if (Record.NO_NEXT_RELATIONSHIP.is(event.nextRel)) return;

        CompactLongRecord2 relationshipsToUpdate = event.relationshipsToUpdate;

        long prevId = event.prevId;

        long followingNextRelationshipId = relationshipsToUpdate.firstNegative();

        prevId = createUpdateRecords(relationshipsToUpdate.getOutgoing(), prevId, followingNextRelationshipId,true);

        followingNextRelationshipId = Record.NO_NEXT_RELATIONSHIP.intValue();

        createUpdateRecords(relationshipsToUpdate.getIncoming(), prevId, followingNextRelationshipId, false);

        if (endOfBatch) relationshipFileUpdater.flush();

        event.relationshipsToUpdate = null;
    }

    private long createUpdateRecords(long[][] relIds, long prevId, long followingNextRelationshipId, boolean outgoing) throws IOException {
        if (relIds==null || relIds.length==0) return prevId;
        int count = relIds.length;
        for (int i = 0; i < count; i++) {
            long nextId = i + 1 < count ? relIds[i + 1][0] : followingNextRelationshipId;
            relationshipFileUpdater.update(relIds[i][0], outgoing, prevId, nextId,(int)relIds[i][1]);
            prevId = relIds[i][0];
            counter++;
        }
        return prevId;
    }

    @Override
    public String toString() {
        return "rel-file-updater  " + counter + " "+ relationshipFileUpdater;
    }
    public void close() {
        relationshipFileUpdater.close();
    }
}
