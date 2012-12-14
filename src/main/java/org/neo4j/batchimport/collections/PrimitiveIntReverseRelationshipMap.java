package org.neo4j.batchimport.collections;

import edu.ucla.sspace.util.primitive.IntIntHashMultiMap;
import edu.ucla.sspace.util.primitive.IntIntMultiMap;
import edu.ucla.sspace.util.primitive.IntSet;

/**
* @author mh
* @since 27.10.12
*/
public class PrimitiveIntReverseRelationshipMap { // implements ReverseRelationshipMap {
    private final IntIntMultiMap inner=new IntIntHashMultiMap();

    public void add(int key, int value) {
        inner.put(key,value);
    }

    public int[] remove(int key) {
        IntSet relIds = inner.remove(key);
        if (relIds==null) return null;
        return relIds.toPrimitiveArray();
    }
}
