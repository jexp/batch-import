package org.neo4j.batchimport.collections;

import java.util.concurrent.ConcurrentHashMap;

/**
* @author mh
* @since 27.10.12
*/
public class ConcurrentLongReverseRelationshipMap implements ReverseRelationshipMap {
    private final ConcurrentHashMap<Long,CompactLongRecord2> inner=new ConcurrentHashMap<Long,CompactLongRecord2>();

    public void add(long key, long value, boolean outgoing, int typeId) {
        CompactLongRecord2 ids = inner.get(key);
        if (ids==null) {
            ids = new CompactLongRecord2((byte)0);
            inner.put(key, ids);
        }
        ids.add(value,outgoing,typeId);
    }

    public CompactLongRecord2 retrieve(long key) {
        return inner.remove(key);
    }

}
