package org.neo4j.batchimport.handlers;

import com.lmax.disruptor.EventHandler;
import org.neo4j.batchimport.collections.CompactLongRecord;
import org.neo4j.batchimport.collections.ConcurrentLongReverseRelationshipMap;
import org.neo4j.batchimport.collections.ReverseRelationshipMap;
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

        // store reverse node-id to rel-id for future updates of relationship-records
    // todo reuse and pool the CompactLongRecords, so we can skip IntArray creation
    final ReverseRelationshipMap futureModeRelIdQueue = new ConcurrentLongReverseRelationshipMap();
    private final RelationshipUpdateCache cache;

    public RelationshipWriteHandler(RelationshipWriter relationshipWriter, final long totalNrOfRels) {
        this.relationshipWriter = relationshipWriter;
        cache = new RelationshipUpdateCache(relationshipWriter, totalNrOfRels);
    }

    @Override
    public void onEvent(NodeStruct event, long nodeId, boolean endOfBatch) throws Exception {

        CompactLongRecord relationshipsToUpdate = futureModeRelIdQueue.retrieve(nodeId);

        event.nextRel = firstRelationshipId(event,relationshipsToUpdate);

        if (Record.NO_NEXT_RELATIONSHIP.is(event.nextRel)) return;

        long maxRelationshipId = maxRelationshipId(event,relationshipsToUpdate);

        relationshipWriter.start(maxRelationshipId);

        int count = event.relationshipCount;
        long followingNextRelationshipId = Record.NO_NEXT_RELATIONSHIP.intValue();
        if (relationshipsToUpdate !=null) {
            long value = relationshipsToUpdate.firstPositive();
            if (value == -1) value = relationshipsToUpdate.firstNegative();
            if (value != -1) followingNextRelationshipId = value;
        }

        long prevId = Record.NO_PREV_RELATIONSHIP.intValue();
        for (int i = 0; i < count; i++) {
            long nextId = i+1 < count ? event.getRelationship(i+1).id : followingNextRelationshipId;
            Relationship relationship = event.getRelationship(i);
            relationshipWriter.create(nodeId,event, relationship, prevId, nextId);
            prevId = relationship.id;
            storeFutureRelId(nodeId, relationship,prevId);

            counter++;
        }

        event.clearRelationshipInfo();

        if (relationshipsToUpdate!=null) {

            followingNextRelationshipId = relationshipsToUpdate.firstNegative();

            prevId = createUpdateRecords(relationshipsToUpdate.getOutgoing(), prevId, followingNextRelationshipId,true);

            followingNextRelationshipId = Record.NO_NEXT_RELATIONSHIP.intValue();

            createUpdateRecords(relationshipsToUpdate.getIncoming(), prevId, followingNextRelationshipId, false);
        }
        //   if (endOfBatch) relationshipWriter.flush();
    }

    private long createUpdateRecords(long[] relIds, long prevId, long followingNextRelationshipId, boolean outgoing) throws IOException {
        if (relIds==null || relIds.length==0) return prevId;
        int count = relIds.length;
        for (int i = 0; i < count; i++) {
            long nextId = i+1 < count ? relIds[i + 1] : followingNextRelationshipId;

            cache.update(relIds[i], outgoing, prevId, nextId);
            //relationshipWriter.update(relIds[i], outgoing, prevId, nextId);
            prevId = relIds[i];
            counter++;
        }
        return prevId;
    }

    @Override
    public String toString() {
        return "rel-record-writer  " + counter + " \n"+relationshipWriter+" "+cache;
    }
    public void close() {
        try {
            cache.close();
            relationshipWriter.close();
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
    }


    private void storeFutureRelId(long nodeId, Relationship relationship, long relId) {
        long other = relationship.other();
        if (other < nodeId) return;
        final boolean otherDirection = !relationship.outgoing();
        futureModeRelIdQueue.add(other, relId, otherDirection);
    }

    private long firstRelationshipId(NodeStruct event, CompactLongRecord relationshipsToUpdate) {
        if (event.relationshipCount>0) return event.getRelationship(0).id;
        if (relationshipsToUpdate !=null) {
            long outgoingRelId = relationshipsToUpdate.firstPositive();
            if (outgoingRelId!=-1) return outgoingRelId;
            return relationshipsToUpdate.firstNegative();
        }
        return Record.NO_PREV_RELATIONSHIP.intValue();
    }

    private long maxRelationshipId(NodeStruct event, CompactLongRecord relationshipsToUpdate) {
        long result=Record.NO_NEXT_RELATIONSHIP.intValue();

        if (relationshipsToUpdate !=null) result=Math.max(relationshipsToUpdate.last(),result); // TODO max of both directions or just the last rel-id that was added, i.e. the biggest
        if (event.relationshipCount>0) result=Math.max(event.getRelationship(event.relationshipCount-1).id,result);
        return result;
    }

}
