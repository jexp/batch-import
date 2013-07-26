package org.neo4j.batchimport.handlers;

import com.lmax.disruptor.EventHandler;
import org.neo4j.batchimport.structs.NodeStruct;
import org.neo4j.kernel.impl.nioneo.store.NodeRecord;
import org.neo4j.kernel.impl.nioneo.store.NodeStore;

/**
* @author mh
* @since 27.10.12
*/
public class NodeWriteRecordHandler implements EventHandler<NodeStruct> {

    long counter = 0;
    private final NodeStore nodeStore;

    public NodeWriteRecordHandler(NodeStore nodeStore) {
        this.nodeStore = nodeStore;
    }

    public void onEvent(NodeStruct event, long nodeId, boolean endOfBatch) throws Exception {
        counter++;
        if (nodeStore.getHighId() <= nodeId) nodeStore.setHighId(nodeId+1);
        nodeStore.updateRecord(createRecord(event, nodeId));
        //if (endOfBatch) nodeStore.flushAll();
    }

    private NodeRecord createRecord(NodeStruct event, long id) {
        NodeRecord record = new NodeRecord(id, event.firstRel, event.firstPropertyId);
        record.setInUse(true);
        record.setCreated();
        return record;
    }

    @Override
    public String toString() {
        return "WritingEventHandler " + counter;
    }

    public void close() {
        nodeStore.flushAll();
    }
}
