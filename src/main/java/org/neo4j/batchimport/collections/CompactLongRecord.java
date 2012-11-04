package org.neo4j.batchimport.collections;

import edu.ucla.sspace.util.primitive.IntIterator;

import java.util.Arrays;

/**
* @author mh
* @since 03.11.12
*/
public class CompactLongRecord {
    private final byte highbit;
    volatile CompactLongRecord next;
    private final IntArray values = new IntArray();
    private final int[] counts=new int[2];

    CompactLongRecord(byte highbit) {
        this.highbit=highbit;
    }

    public void add(long id, boolean outgoing) {
        final byte lead = (byte)(id >> 31L);
        add(lead,id,outgoing);
    }

    private void add(byte lead, long id, boolean outgoing) {
        if (lead!=highbit) {
            if (next==null) {
                next=new CompactLongRecord((byte)(highbit+1));
            }
            next.add(lead,id,outgoing);
        } else {
            int value = (int)(id & 0x7FFFFFFF);
            if (!outgoing) value = ~value;
            counts[idx(outgoing)]++;
            values.add(value);
        }
    }

    public long[] getOutgoing() {
        return get(true);
    }

    public long[] getIncoming() {
        return get(false);
    }

    private long[] get(boolean outgoing) {
        long[] result=new long[count(outgoing)];
        return collect(result,outgoing,0);
    }

    private long[] collect(long[] result, boolean positive, int offset) {
        final int max = counts[idx(positive)];
        int i=offset;
        for (int j = 0; j < values.count; j++) {
            int value = values.data[j];
            final boolean isPositive = value >= 0;
            if (isPositive == positive) {
                result[i++]= convert(value);
            }
        }
        if (offset+max!=i) throw new IllegalStateException(String.format("Max %d != array counter %d",max,i));
        if (next!=null) return next.collect(result,positive,i);
        // Arrays.sort(result);
        return result;
    }

    public int count(boolean outgoing) {
        return counts[idx(outgoing)] + (next!=null ? next.count(outgoing) : 0);
    }

    private int idx(boolean outgoing) {
        return outgoing?0:1;
    }

    public long first() {
       if (values.hasData()) {
           return convert(values.first());
       }
       if (next!=null) return next.first();
       return -1L;
    }

    public long firstPositive() {
       if (values.hasPositive()) {
           return convert(values.firstPositive());
       }
       if (next!=null) return next.firstPositive();
       return -1L;
    }

    public long firstNegative() {
       if (values.hasNegative()) {
           return convert(values.firstNegative());
       }
       if (next!= null) return next.firstNegative();
       return -1L;
    }

    public long last() {
        if (next!=null) return next.first();
        if (values.hasData()) {
            return convert(values.last());
        }
       return -1L;
    }

    private long convert(int value) {
        if (value < 0) value = ~value;
        return (long)highbit<<31 | value;
    }
}
