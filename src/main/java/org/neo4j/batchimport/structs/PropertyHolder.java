package org.neo4j.batchimport.structs;

import org.neo4j.kernel.impl.nioneo.store.PropertyRecord;
import org.neo4j.kernel.impl.nioneo.store.Record;

import java.util.Arrays;

/**
* @author mh
* @since 27.10.12
*/
public class PropertyHolder {
    public volatile long id;
    public volatile long firstPropertyId = Record.NO_NEXT_PROPERTY.intValue();

    public volatile int propertyCount;
    public final Property[] properties;
    public final PropertyRecord[] propertyRecords;

    public PropertyHolder(int propertyCount) {
        this.properties = new Property[propertyCount];
        for (int i = 0; i < properties.length; i++) {
            properties[i]=new Property();
        }
        this.propertyRecords =new PropertyRecord[propertyCount];
    }
    public NodeStruct init() {
        id = 0;
        firstPropertyId = Record.NO_NEXT_PROPERTY.intValue();
        propertyCount = 0;
        return null;
    }
    public void addProperty(int id, Object value) {
        this.properties[propertyCount++].init(id,value);
    }

    public void cleanProperties() {
        for (Property property : properties) {
            property.cleanValue();
            property.cleanBlock();
        }
        Arrays.fill(propertyRecords,null);
    }
}
