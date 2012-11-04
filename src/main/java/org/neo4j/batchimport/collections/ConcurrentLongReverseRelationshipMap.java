package org.neo4j.batchimport.collections;

import edu.ucla.sspace.util.primitive.CompactIntSet;
import edu.ucla.sspace.util.primitive.IntSet;

import java.util.Arrays;
import java.util.concurrent.ConcurrentHashMap;

/**
* @author mh
* @since 27.10.12
*/
public class ConcurrentLongReverseRelationshipMap implements ReverseRelationshipMap {
    private final ConcurrentHashMap<Long,CompactLongRecord> inner=new ConcurrentHashMap<Long,CompactLongRecord>();

    public void add(long key, long value, boolean outgoing) {
        CompactLongRecord ids = inner.get(key);
        if (ids==null) {
            ids = new CompactLongRecord((byte)0);
            inner.put(key, ids);
        }
        ids.add(value,outgoing);
    }

    public CompactLongRecord retrieve(long key) {
        return inner.remove(key);
    }

}
