package org.neo4j.batchimport.handlers;

import com.lmax.disruptor.EventHandler;
import org.neo4j.batchimport.NodeStructFactory;
import org.neo4j.batchimport.collections.ConcurrentLongReverseRelationshipMap;
import org.neo4j.batchimport.collections.PrimitiveIntReverseRelationshipMap;
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
    final ReverseRelationshipMap futureModeRelIdQueueOutgoing;
    final ReverseRelationshipMap futureModeRelIdQueueIncoming;

    public RelationshipIdHandler(int relsPerNode) {
        futureModeRelIdQueueOutgoing = new ConcurrentLongReverseRelationshipMap(relsPerNode);
        futureModeRelIdQueueIncoming = new ConcurrentLongReverseRelationshipMap(relsPerNode);
    }
//    final ReverseRelationshipMap futureModeRelIdQueueOutgoing = new PrimitiveIntReverseRelationshipMap();
//    final ReverseRelationshipMap futureModeRelIdQueueIncoming = new PrimitiveIntReverseRelationshipMap();
    //final ReverseRelationshipMap futureModeRelIdQueueOutgoing = new ConcurrentReverseRelationshipMap(RELS_PER_NODE);
    //final ReverseRelationshipMap futureModeRelIdQueueIncoming = new ConcurrentReverseRelationshipMap(RELS_PER_NODE);

    public void onEvent(NodeStruct event, long nodeId, boolean endOfBatch) throws Exception {
        for (int i = 0; i < event.relationshipCount; i++) {
            Relationship relationship = event.getRelationship(i);
            long relId = this.relId++;
            relationship.id = relId;
            storeFutureRelId(nodeId, relationship,relId);
        }

        event.outgoingRelationshipsToUpdate = futureRelIds(nodeId, futureModeRelIdQueueOutgoing);
        event.incomingRelationshipsToUpdate = futureRelIds(nodeId, futureModeRelIdQueueIncoming);
        event.nextRel = firstRelationshipId(event);
        event.maxRelationshipId = maxRelationshipId(event);
    }

    private void storeFutureRelId(long nodeId, Relationship relationship, long relId) {
        long other = relationship.other();
        if (other < nodeId) return;
        if (relationship.outgoing()) {
            futureModeRelIdQueueIncoming.add(other, relId);
        } else {
            futureModeRelIdQueueOutgoing.add(other, relId);
        }
    }

    private long[] futureRelIds(long nodeId, ReverseRelationshipMap futureRelIds) {
        long[] relIds = futureRelIds.remove(nodeId);
        if (relIds == null) return null;
        return relIds;
    }

    private long firstRelationshipId(NodeStruct event) {
        if (event.relationshipCount>0) return event.getRelationship(0).id;
        if (event.outgoingRelationshipsToUpdate!=null) return event.outgoingRelationshipsToUpdate[0];
        if (event.incomingRelationshipsToUpdate!=null) return event.incomingRelationshipsToUpdate[0];
        return Record.NO_PREV_RELATIONSHIP.intValue();
    }

    private long maxRelationshipId(NodeStruct event) {
        long result=Record.NO_NEXT_RELATIONSHIP.intValue();

        if (event.incomingRelationshipsToUpdate!=null) result=Math.max(event.incomingRelationshipsToUpdate[Utils.size(event.incomingRelationshipsToUpdate)-1],result);
        if (event.outgoingRelationshipsToUpdate!=null) result=Math.max(event.outgoingRelationshipsToUpdate[Utils.size(event.outgoingRelationshipsToUpdate)-1],result);
        if (event.relationshipCount>0) result=Math.max(event.getRelationship(event.relationshipCount-1).id,result);
        return result;
    }

    @Override
    public String toString() {
        return "relId: " + relId;
    }
}
