package org.neo4j.batchimport.collections;

import edu.ucla.sspace.util.primitive.IntSet;

import java.util.Arrays;
import java.util.concurrent.ConcurrentHashMap;

/**
* @author mh
* @since 27.10.12
*/
public class ConcurrentLongReverseRelationshipMap implements ReverseRelationshipMap {
    private final ConcurrentHashMap<Long,long[]> inner=new ConcurrentHashMap<Long,long[]>();
    private final int arraySize;

    public ConcurrentLongReverseRelationshipMap(int arraySize) {
        this.arraySize = arraySize;
    }

    public void add(long key, long value) {
        long[] ids = inner.get(key);
        if (ids==null) {
            ids = new long[arraySize];
            Arrays.fill(ids, -1);
            inner.put(key, ids);
        }
        for (int i=0;i<arraySize;i++) {
            if (ids[i]==-1) {
                ids[i]=value;
                return;
            }
        }
        throw new ArrayIndexOutOfBoundsException("Already "+arraySize+" values in array "+Arrays.toString(ids));
    }

    public long[] remove(long key) {
        return inner.remove(key);
    }

}
