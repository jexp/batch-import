package org.neo4j.batchimport.handlers;

import com.lmax.disruptor.EventHandler;
import org.neo4j.batchimport.collections.ConcurrentLongReverseRelationshipMap;
import org.neo4j.batchimport.Utils;
import org.neo4j.batchimport.collections.ReverseRelationshipMap;
import org.neo4j.batchimport.structs.NodeStruct;
import org.neo4j.batchimport.structs.Relationship;
import org.neo4j.kernel.impl.nioneo.store.Record;

/**
* @author mh
* @since 27.10.12
*/
public class RelationshipIdHandler implements EventHandler<NodeStruct> {
    volatile long relId = 0;
    // store reverse node-id to rel-id for future updates of relationship-records
    // todo reuse and pool the CompactLongRecords, so we can skip IntArray creation
    final ReverseRelationshipMap futureModeRelIdQueue = new ConcurrentLongReverseRelationshipMap();

    public void onEvent(NodeStruct event, long nodeId, boolean endOfBatch) throws Exception {
        for (int i = 0; i < event.relationshipCount; i++) {
            Relationship relationship = event.getRelationship(i);
            long relId = this.relId++;
            relationship.id = relId;
            storeFutureRelId(nodeId, relationship,relId);
        }

        event.relationshipsToUpdate = futureModeRelIdQueue.retrieve(nodeId);

        event.nextRel = firstRelationshipId(event);
        event.maxRelationshipId = maxRelationshipId(event);
    }

    private void storeFutureRelId(long nodeId, Relationship relationship, long relId) {
        long other = relationship.other();
        if (other < nodeId) return;
        final boolean otherDirection = !relationship.outgoing();
        futureModeRelIdQueue.add(other, relId, otherDirection);
    }

    private long firstRelationshipId(NodeStruct event) {
        if (event.relationshipCount>0) return event.getRelationship(0).id;
        if (event.relationshipsToUpdate !=null) {
            long outgoingRelId = event.relationshipsToUpdate.firstPositive();
            if (outgoingRelId!=-1) return outgoingRelId;
            return event.relationshipsToUpdate.firstNegative();
        }
        return Record.NO_PREV_RELATIONSHIP.intValue();
    }

    private long maxRelationshipId(NodeStruct event) {
        long result=Record.NO_NEXT_RELATIONSHIP.intValue();

        if (event.relationshipsToUpdate !=null) result=Math.max(event.relationshipsToUpdate.last(),result); // TODO max of both directions or just the last rel-id that was added, i.e. the biggest
        if (event.relationshipCount>0) result=Math.max(event.getRelationship(event.relationshipCount-1).id,result);
        return result;
    }

    @Override
    public String toString() {
        return "relId: " + relId;
    }
}
