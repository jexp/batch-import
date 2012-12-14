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
    public final PropertyBlock block = new PropertyBlock();

    void init(int index, Object value) {
        this.nameIndex = index;
        this.value = value;
        cleanBlock();
    }

    public void cleanBlock() {
        this.block.clean();
    }

    public void encode(PropertyStore propStore) {
        propStore.encodeValue(block, nameIndex, value);
    }

    public void cleanValue() {
        this.value = null;
    }
}
