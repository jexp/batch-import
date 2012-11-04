package org.neo4j.batchimport.collections;

/**
* @author mh
* @since 03.11.12
*/
public class IntArray {
    volatile int[] data;
    volatile int count;
    volatile int negative;

    public IntArray() {
        this(64);
    }

    public IntArray(final int initial) {
        this.data = new int[initial];
    }
    void add(int value) {
        if (count == data.length) {
            int[] newData = new int[data.length*2];
            System.arraycopy(data,0,newData,0,data.length);
            data = newData;
        }
        data[count++]=value;
        if (value<0) negative++;
    }

    public int first() {
        if (hasData()) return data[0];
        throw new ArrayIndexOutOfBoundsException("No data");
    }

    public int firstNegative() {
        if (hasNegative()) {
            for (int i = 0; i < count; i++) {
                if (data[i]<0) return data[i];
            }
        }
        throw new ArrayIndexOutOfBoundsException("No data");
    }

    public int firstPositive() {
        if (hasPositive()) {
            for (int i = 0; i < count; i++) {
                if (data[i] >= 0) return data[i];
            }
        }
        throw new ArrayIndexOutOfBoundsException("No data");
    }

    public boolean hasData() {
        return count > 0;
    }

    public boolean hasNegative() {
        return negative>0;
    }

    public boolean hasPositive() {
        return (count-negative) > 0;
    }

    public int last() {
        if (hasData()) return data[count-1];
        throw new ArrayIndexOutOfBoundsException("No data");
    }
}
