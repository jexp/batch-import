package org.neo4j.batchimport.handlers;

import com.lmax.disruptor.EventHandler;
import org.neo4j.batchimport.structs.NodeStruct;
import org.neo4j.batchimport.structs.Relationship;
import org.neo4j.kernel.impl.nioneo.store.Record;

import java.io.*;

/**
* @author mh
* @since 27.10.12
*/
public class RelationshipWriteHandler implements EventHandler<NodeStruct> {
    private long counter;
    private final RelationshipWriter relationshipWriter;

    // ForwardRelationshipUpdateManager futureNodeRelInfo;
    // private final RelationshipUpdateCache cache;

    public RelationshipWriteHandler(RelationshipWriter relationshipWriter, final long totalNrOfRels) {
        this.relationshipWriter = relationshipWriter;
//        cache = new RelationshipUpdateCache(relationshipWriter, totalNrOfRels);
//        futureNodeRelInfo = new ForwardRelationshipUpdateManager(cache);
    }

    @Override
    public void onEvent(NodeStruct event, long nodeId, boolean endOfBatch) throws Exception {

        // event.firstRel = firstRelationshipId(event,futureNodeRelInfo.getFirstRelId(nodeId));

        if (Record.NO_NEXT_RELATIONSHIP.is(event.firstRel)) return;

        long maxRelationshipId = maxRelationshipId(event);

        relationshipWriter.start(maxRelationshipId);

        int count = event.relationshipCount;

        long followingNextRelationshipId = Record.NO_NEXT_RELATIONSHIP.intValue();

        // long prevId = futureNodeRelInfo.done(nodeId, getFirstOwnRelationshipId(event));
        long prevId = event.prevId;

        for (int i = 0; i < count; i++) {
            long nextId = i+1 < count ? event.getRelationship(i+1).id : followingNextRelationshipId;
            Relationship relationship = event.getRelationship(i);
            relationshipWriter.create(nodeId,event, relationship, prevId, nextId);
            prevId = relationship.id;
            // storeFutureRelId(nodeId, relationship,prevId);

            counter++;
        }

        //   if (endOfBatch) relationshipWriter.flush();
    }

    @Override
    public String toString() {
        return "rel-record-writer  " + counter + " \n"+relationshipWriter; // +" "+cache;
    }
    public void close() {
        try {
            // cache.close();
            relationshipWriter.close();
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
    }

   /*
    private void storeFutureRelId(long nodeId, Relationship relationship, long relId) throws IOException {
        long other = relationship.other();
        if (other < nodeId) return;
        final boolean otherDirection = !relationship.outgoing();
        futureNodeRelInfo.add(other,relId,otherDirection);
    }

    private long firstRelationshipId(NodeStruct event, Long firstFutureId) {
        if (firstFutureId!=null) return firstFutureId;
        return getFirstOwnRelationshipId(event);
    }

    private long getFirstOwnRelationshipId(NodeStruct event) {
        if (event.relationshipCount == 0) return Record.NO_PREV_RELATIONSHIP.intValue();
        return event.getRelationship(0).id;
    }
   */


    private long maxRelationshipId(NodeStruct event) {
        if (event.relationshipCount == 0) return Record.NO_NEXT_RELATIONSHIP.intValue();
        return event.getRelationship(event.relationshipCount-1).id;
    }

}
