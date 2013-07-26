package org.neo4j.batchimport.handlers;

import com.lmax.disruptor.EventHandler;
import org.neo4j.batchimport.structs.NodeStruct;
import org.neo4j.batchimport.structs.Relationship;
import org.neo4j.kernel.impl.nioneo.store.Record;

import java.io.IOException;

/**
* @author mh
* @since 27.10.12
*/
public class ForwardRelationshipUpdateHandler implements EventHandler<NodeStruct> {
    private long counter;

    private final ForwardRelationshipUpdateManager futureNodeRelInfo;
    private final RelationshipUpdateCache cache;

    public ForwardRelationshipUpdateHandler(RelationshipWriter relationshipWriter, final long totalNrOfRels, final int relsPerBuffer) {
        cache = new RelationshipUpdateCache(relationshipWriter, totalNrOfRels,RelationshipUpdateCache.BUCKETS,relsPerBuffer);
        futureNodeRelInfo = new ForwardRelationshipUpdateManager(cache);
    }

    @Override
    public void onEvent(NodeStruct event, long nodeId, boolean endOfBatch) throws Exception {
        event.firstRel = firstRelationshipId(event,futureNodeRelInfo.getFirstRelId(nodeId));

        if (Record.NO_NEXT_RELATIONSHIP.is(event.firstRel)) return;

        event.prevId = futureNodeRelInfo.done(nodeId, getFirstOwnRelationshipId(event));

        int count = event.relationshipCount;

        for (int i = 0; i < count; i++) {
            Relationship relationship = event.getRelationship(i);
            storeFutureRelId(nodeId, relationship);

            counter++;
        }
    }

    private long firstRelationshipId(NodeStruct event, Long firstFutureId) {
        if (firstFutureId!=null) return firstFutureId;
        return getFirstOwnRelationshipId(event);
    }

    private long getFirstOwnRelationshipId(NodeStruct event) {
        if (event.relationshipCount == 0) return Record.NO_PREV_RELATIONSHIP.intValue();
        return event.getRelationship(0).id;
    }


    @Override
    public String toString() {
        return "rel-update-handler " + counter + " "+cache;
    }

    public void close() {
        try {
            cache.close();
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
    }

    private void storeFutureRelId(long nodeId, Relationship relationship) throws IOException {
        long other = relationship.other();
        if (other <= nodeId) return;
        final boolean otherDirection = !relationship.outgoing();
        futureNodeRelInfo.add(other,relationship.id,otherDirection);
    }
}
