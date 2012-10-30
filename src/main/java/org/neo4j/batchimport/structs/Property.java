package org.neo4j.batchimport.structs;

import org.neo4j.kernel.impl.nioneo.store.PropertyBlock;
import org.neo4j.kernel.impl.nioneo.store.PropertyStore;

/**
* @author mh
* @since 27.10.12
*/
public class Property {
    public volatile int nameIndex;
    public volatile Object value;
    public volatile PropertyBlock block;

    void init(int index, Object value) {
        this.nameIndex =index;
        this.value = value;
        this.block = null;
    }
    public void encode(PropertyStore propStore) {
        PropertyBlock block = new PropertyBlock();
        propStore.encodeValue(block, nameIndex, value);
        this.block = block;
    }
}
