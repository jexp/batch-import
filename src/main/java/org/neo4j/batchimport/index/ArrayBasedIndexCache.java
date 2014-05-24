package org.neo4j.batchimport.index;

import java.util.Arrays;

/**
 * @author mh
 * @since 20.05.14
 */
public class ArrayBasedIndexCache implements IndexCache {
    private Object[] data;
    private int offset=0;
    private String index;
    private int capacity;

    public ArrayBasedIndexCache(String index,int capacity) {
        this.index = index;
        this.capacity = capacity;
        data = new Object[capacity];
    }

    @Override
    public void add(Object value) {
        if (offset==capacity) {
            capacity *= 2;
            data = Arrays.copyOf(data, capacity);
        }
        data[offset++]=value;
    }

    @Override
    public void doneInsert() {
        data = Arrays.copyOf(data,offset); // todo only when capacity >>> offset
        Arrays.sort(data,0,offset);
    }

    @Override
    public int get(Object value) {
        int idx = Arrays.binarySearch(data, 0, offset, value);
        return idx < 0 ? -1 : idx;
    }

    @Override
    public void set(Object value, long id) {
        if (id > Integer.MAX_VALUE) throw new ArrayIndexOutOfBoundsException("Don't support values > " + Integer.MAX_VALUE);
        if (id >= capacity) {
            capacity = id > capacity * 2 ? (int) (id * 1.1) : capacity * 2;
            data = Arrays.copyOf(data, capacity);
        }
        data[(int)id]=value;
    }
}
