package org.neo4j.batchimport.collections;

import java.util.Arrays;
import java.util.concurrent.ConcurrentHashMap;

/**
* @author mh
* @since 27.10.12
*/
public class ConcurrentReverseRelationshipMap implements ReverseRelationshipMap {
    private final ConcurrentHashMap<Integer,int[]> inner=new ConcurrentHashMap<Integer,int[]>();
    private final int arraySize;

    ConcurrentReverseRelationshipMap(int arraySize) {
        this.arraySize = arraySize;
    }

    public void add(int key, int value) {
        int[] ints = inner.get(key);
        if (ints==null) {
            ints = new int[arraySize];
            Arrays.fill(ints, -1);
            inner.put(key, ints);
        }
        for (int i=0;i<arraySize;i++) {
            if (ints[i]==-1) {
                ints[i]=value;
                return;
            }
        }
        throw new ArrayIndexOutOfBoundsException("Already "+arraySize+" values in array "+Arrays.toString(ints));
    }

    public int[] remove(int key) {
        return inner.remove(key);
    }

}
