package org.neo4j.batchimport.handlers;

import com.lmax.disruptor.EventHandler;
import org.neo4j.batchimport.structs.NodeStruct;
import org.neo4j.batchimport.structs.PropertyHolder;
import org.neo4j.kernel.impl.nioneo.store.PropertyRecord;
import org.neo4j.kernel.impl.nioneo.store.PropertyStore;

/**
* @author mh
* @since 27.10.12
*/
public class PropertyWriteRecordHandler implements EventHandler<NodeStruct> {

    long counter = 0;
    private final PropertyStore propStore;

    public PropertyWriteRecordHandler(PropertyStore propStore) {
        this.propStore = propStore;
    }

    public void onEvent(NodeStruct event, long nodeId, boolean endOfBatch) throws Exception {
        if (propStore.getHighId() <= event.lastPropertyId) propStore.setHighId(event.lastPropertyId);
        writePropertyRecords(event);
        for (int i = 0; i < event.relationshipCount; i++) {
            writePropertyRecords(event.getRelationship(i));
        }
        // if (endOfBatch) propStore.flushAll();
    }

    private void writePropertyRecords(PropertyHolder holder) {
        if (holder.propertyCount==0) return;

        for (int i=0;i<holder.propertyCount;i++) {
            PropertyRecord record = holder.propertyRecords[i];
            if (record == null) break;
            propStore.updateRecord(record);
            counter++;
        }
        holder.cleanProperties();
    }

    @Override
    public String toString() {
        return "PropertyWritingEventHandler " + counter;
    }

    public void close() {
        propStore.flushAll();
    }

}
