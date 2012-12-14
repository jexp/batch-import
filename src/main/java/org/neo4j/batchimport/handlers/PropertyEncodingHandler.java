package org.neo4j.batchimport.handlers;

import com.lmax.disruptor.EventHandler;
import org.neo4j.batchimport.structs.NodeStruct;
import org.neo4j.batchimport.structs.Property;
import org.neo4j.batchimport.structs.PropertyHolder;
import org.neo4j.kernel.impl.nioneo.store.PropertyStore;
import org.neo4j.unsafe.batchinsert.BatchInserterImpl;

/**
* @author mh
* @since 27.10.12
*/
public class PropertyEncodingHandler implements EventHandler<NodeStruct> {
    private long count;
    private final int pos;
    private final PropertyStore propStore;
    public static final int MASK = 1;

    public PropertyEncodingHandler(BatchInserterImpl inserter, int pos) {
        this.pos = pos;
        propStore = inserter.getPropertyStore();
    }

    public static PropertyEncodingHandler[] createHandlers(final BatchInserterImpl inserter) {
        PropertyEncodingHandler[] propertyMappingHandlers = new PropertyEncodingHandler[MASK + 1];
        for (int i = 0; i < propertyMappingHandlers.length; i++) {
            propertyMappingHandlers[i] = new PropertyEncodingHandler(inserter, i);
        }
        return propertyMappingHandlers;
    }

    public void onEvent(NodeStruct event, long nodeId, boolean endOfBatch) throws Exception {
        if ((nodeId & MASK) != pos) return;
        encodeProperties(event);
        for (int i = 0; i < event.relationshipCount; i++) {
             encodeProperties(event.getRelationship(i));
        }
    }

    private void encodeProperties(PropertyHolder holder) {
        if (holder.propertyCount ==0) return;
        // todo cache encoded blocks in an LRU cache
        for (int id = 0; id < holder.propertyCount; id++) {
            Property value = holder.properties[id];
            value.encode(propStore);
            count++;
        }
    }

    @Override
    public String toString() {
        return "encoded "+count+" properties";
    }
}
