package org.neo4j.batchimport.handlers;

import com.lmax.disruptor.EventHandler;
import org.neo4j.batchimport.structs.NodeStruct;
import org.neo4j.batchimport.structs.Property;
import org.neo4j.batchimport.structs.PropertyHolder;
import org.neo4j.kernel.impl.nioneo.store.PropertyBlock;
import org.neo4j.kernel.impl.nioneo.store.PropertyRecord;
import org.neo4j.kernel.impl.nioneo.store.PropertyType;

/**
* @author mh
* @since 27.10.12
*/
public class PropertyRecordCreatorHandler implements EventHandler<NodeStruct> {
    public static final int PAYLOAD_SIZE = PropertyType.getPayloadSize();
    private volatile long propertyId=0;

    @Override
    public void onEvent(NodeStruct event, long nodeId, boolean endOfBatch) throws Exception {
        createPropertyRecords(event);
        for (int i = 0; i < event.relationshipCount; i++) {
            createPropertyRecords(event.getRelationship(i));
        }
        event.lastPropertyId = propertyId;
    }

    private void createPropertyRecords(PropertyHolder holder) {
        if (holder.propertyCount==0) return;
        holder.firstPropertyId = propertyId;
        PropertyRecord currentRecord = createRecord(propertyId);
        propertyId++;
        int index=0;
        holder.propertyRecords[index++] = currentRecord;
        for (int i = 0; i < holder.propertyCount; i++) {
            Property property = holder.properties[i];
            PropertyBlock block = property.block;
            if (currentRecord.size() + block.getSize() > PAYLOAD_SIZE){
                currentRecord.setNextProp(propertyId);
                currentRecord = createRecord(propertyId);
                currentRecord.setPrevProp(propertyId-1);
                propertyId++;
                holder.propertyRecords[index++] = currentRecord;
            }
            currentRecord.addPropertyBlock(block);
            property.cleanValue();
        }
        if (index<holder.propertyRecords.length) holder.propertyRecords[index]=null;
    }

    private PropertyRecord createRecord(long id) {
        PropertyRecord currentRecord = new PropertyRecord(id);
        currentRecord.setInUse( true );
        currentRecord.setCreated();
        return currentRecord;
    }

    @Override
    public String toString() {
        return "MaxPropertyId "+propertyId;
    }
}
