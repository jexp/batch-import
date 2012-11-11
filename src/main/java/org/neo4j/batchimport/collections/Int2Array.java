package org.neo4j.batchimport.collections;

/**
* @author mh
* @since 03.11.12
*/
public class Int2Array {
    volatile int[][] data;
    volatile int count;
    volatile int negative;

    public Int2Array() {
        this(64);
    }

    public Int2Array(final int initial) {
        this.data = new int[initial][2];
    }
    void add(int value, int payload) {
        if (count == data.length) {
            int[][] newData = new int[data.length*2][2];
            System.arraycopy(data,0,newData,0,data.length);
            data = newData;
        }
        data[count][0]=value;
        data[count][1]= payload;
        count++;
        if (value<0) negative++;
    }

    public int first() {
        if (hasData()) return data[0][0];
        throw new ArrayIndexOutOfBoundsException("No data");
    }

    public int firstNegative() {
        if (hasNegative()) {
            for (int i = 0; i < count; i++) {
                if (data[i][0]<0) return data[i][0];
            }
        }
        throw new ArrayIndexOutOfBoundsException("No data");
    }

    public int firstPositive() {
        if (hasPositive()) {
            for (int i = 0; i < count; i++) {
                if (data[i][0] >= 0) return data[i][0];
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
        if (hasData()) return data[count-1][0];
        throw new ArrayIndexOutOfBoundsException("No data");
    }
}
