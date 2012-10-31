package org.neo4j.batchimport.handlers;

import com.lmax.disruptor.EventHandler;
import org.neo4j.batchimport.structs.NodeStruct;
import org.neo4j.kernel.impl.nioneo.store.PropertyStore;

/**
* @author mh
* @since 27.10.12
*/
public class PropertyRecordHighIdHandler implements EventHandler<NodeStruct> {
    private final PropertyStore propStore;

    public PropertyRecordHighIdHandler(PropertyStore propStore) {
        this.propStore = propStore;
    }

    public void onEvent(NodeStruct event, long sequence, boolean endOfBatch) throws Exception {
        if (propStore.getHighId()<event.lastPropertyId) propStore.setHighId(event.lastPropertyId);
    }
}
