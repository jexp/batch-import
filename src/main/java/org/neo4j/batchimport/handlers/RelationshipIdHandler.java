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

    public void onEvent(NodeStruct event, long nodeId, boolean endOfBatch) throws Exception {
        for (int i = 0; i < event.relationshipCount; i++) {
            Relationship relationship = event.getRelationship(i);
            relationship.id = relId++;
        }
    }

    @Override
    public String toString() {
        return "relId: " + relId;
    }
}
