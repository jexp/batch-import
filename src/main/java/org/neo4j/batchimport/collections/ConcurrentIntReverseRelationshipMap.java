package org.neo4j.batchimport.collections;

import java.util.Arrays;
import java.util.concurrent.ConcurrentHashMap;

/**
* @author mh
* @since 27.10.12
*/
public class ConcurrentIntReverseRelationshipMap { // implements ReverseRelationshipMap {
    private final ConcurrentHashMap<Integer,int[]> inner=new ConcurrentHashMap<Integer,int[]>();
    private final int arraySize;

    ConcurrentIntReverseRelationshipMap(int arraySize) {
        this.arraySize = arraySize;
    }

    public void add(long key, long value) {
        int[] ints = inner.get((int)key);
        if (ints==null) {
            ints = new int[arraySize];
            Arrays.fill(ints, -1);
            inner.put((int)key, ints);
        }
        for (int i=0;i<arraySize;i++) {
            if (ints[i]==-1) {
                ints[i]=(int)value;
                return;
            }
        }
        throw new ArrayIndexOutOfBoundsException("Already "+arraySize+" values in array "+Arrays.toString(ints));
    }

    public int[] remove(long key) {
        return inner.remove((int)key);
    }

}
