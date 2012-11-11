package org.neo4j.batchimport.handlers;

import com.lmax.disruptor.EventHandler;
import org.neo4j.batchimport.Utils;
import org.neo4j.batchimport.collections.CompactLongRecord;
import org.neo4j.batchimport.collections.CompactLongRecord2;
import org.neo4j.batchimport.collections.ConcurrentLongReverseRelationshipMap;
import org.neo4j.batchimport.collections.ReverseRelationshipMap;
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

        // store reverse node-id to rel-id for future updates of relationship-records
    // todo reuse and pool the CompactLongRecords, so we can skip IntArray creation
    final ReverseRelationshipMap futureModeRelIdQueue = new ConcurrentLongReverseRelationshipMap();

    public RelationshipWriteHandler(RelationshipWriter relationshipWriter) {
        this.relationshipWriter = relationshipWriter;
    }

    @Override
    public void onEvent(NodeStruct event, long nodeId, boolean endOfBatch) throws Exception {

        CompactLongRecord2 relationshipsToUpdate = futureModeRelIdQueue.retrieve(nodeId);

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
//            System.out.printf("Node[%d] %s prev %d next %d%n",nodeId,relationship,prevId,nextId);
            relationshipWriter.create(nodeId,event, relationship, prevId, nextId);
            
            int typeId = computeTypeId(nodeId,relationship, prevId, nextId);
            prevId = relationship.id;
            storeFutureRelId(nodeId, relationship,prevId,typeId);

            counter++;
        }
        
        event.prevId = prevId;

        event.relationshipsToUpdate = relationshipsToUpdate;

        if (endOfBatch) relationshipWriter.flush();

    }

    private int computeTypeId(long nodeId, Relationship relationship, long prevId, long nextId) {
        final int type = relationship.type;
        final boolean outgoing = relationship.outgoing();
        long secondNode = outgoing ? relationship.other() : nodeId;

        long firstPrevRel = outgoing ? prevId : 0;
        long firstNextRel = outgoing ? nextId : 0;

        long secondPrevRel = outgoing ? 0 : prevId;
        long secondNextRel = outgoing ? 0 : nextId;

        long secondNodeMod = (secondNode & 0x700000000L) >> 4;
        long firstPrevRelMod = firstPrevRel == Record.NO_NEXT_RELATIONSHIP.intValue() ? 0 : (firstPrevRel & 0x700000000L) >> 7;
        long firstNextRelMod = firstNextRel == Record.NO_NEXT_RELATIONSHIP.intValue() ? 0 : (firstNextRel & 0x700000000L) >> 10;
        long secondPrevRelMod = secondPrevRel == Record.NO_NEXT_RELATIONSHIP.intValue() ? 0 : (secondPrevRel & 0x700000000L) >> 13;
        long secondNextRelMod = secondNextRel == Record.NO_NEXT_RELATIONSHIP.intValue() ? 0 : (secondNextRel & 0x700000000L) >> 16;
        
        // [ xxx,    ][    ,    ][    ,    ][    ,    ] second node high order bits,     0x70000000
        // [    ,xxx ][    ,    ][    ,    ][    ,    ] first prev rel high order bits,  0xE000000
        // [    ,   x][xx  ,    ][    ,    ][    ,    ] first next rel high order bits,  0x1C00000
        // [    ,    ][  xx,x   ][    ,    ][    ,    ] second prev rel high order bits, 0x380000
        // [    ,    ][    , xxx][    ,    ][    ,    ] second next rel high order bits, 0x70000
        // [    ,    ][    ,    ][xxxx,xxxx][xxxx,xxxx] type
        int typeInt = (int)(relationship.type | secondNodeMod | firstPrevRelMod | firstNextRelMod | secondPrevRelMod | secondNextRelMod);
        return typeInt;
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


    private void storeFutureRelId(long nodeId, Relationship relationship, long relId, int typeId) {
        long other = relationship.other();
        if (other < nodeId) return;
        final boolean otherDirection = !relationship.outgoing();
        futureModeRelIdQueue.add(other, relId, otherDirection,typeId);
    }

    private long firstRelationshipId(NodeStruct event, CompactLongRecord2 relationshipsToUpdate) {
        if (event.relationshipCount>0) return event.getRelationship(0).id;
        if (relationshipsToUpdate !=null) {
            long outgoingRelId = relationshipsToUpdate.firstPositive();
            if (outgoingRelId!=-1) return outgoingRelId;
            return relationshipsToUpdate.firstNegative();
        }
        return Record.NO_PREV_RELATIONSHIP.intValue();
    }

    private long maxRelationshipId(NodeStruct event, CompactLongRecord2 relationshipsToUpdate) {
        long result=Record.NO_NEXT_RELATIONSHIP.intValue();

        if (relationshipsToUpdate !=null) result=Math.max(relationshipsToUpdate.last(),result); // TODO max of both directions or just the last rel-id that was added, i.e. the biggest
        if (event.relationshipCount>0) result=Math.max(event.getRelationship(event.relationshipCount-1).id,result);
        return result;
    }

}
