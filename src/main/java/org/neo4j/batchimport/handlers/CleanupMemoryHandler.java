package org.neo4j.batchimport.handlers;

import com.lmax.disruptor.EventHandler;
import org.neo4j.batchimport.structs.NodeStruct;

/**
 * @author mh
 * @since 11.11.12
 */
public class CleanupMemoryHandler implements EventHandler<NodeStruct> {
    @Override
    public void onEvent(NodeStruct nodeStruct, long nodeId, boolean endOfBatch) throws Exception {
        nodeStruct.clearRelationshipInfo();
        nodeStruct.cleanProperties();
    }
}
