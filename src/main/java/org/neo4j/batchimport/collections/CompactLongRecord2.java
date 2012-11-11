package org.neo4j.batchimport.collections;

/**
* @author mh
* @since 03.11.12
*/
public class CompactLongRecord2 {
    private final byte highbit;
    volatile CompactLongRecord2 next;
    private final Int2Array values = new Int2Array();
    private final int[] counts=new int[2];

    CompactLongRecord2(byte highbit) {
        this.highbit=highbit;
    }

    public void add(long id, boolean outgoing, int typeMod) {
        final byte lead = (byte)(id >> 31L);
        add(lead,id,outgoing,typeMod);
    }

    private void add(byte lead, long id, boolean outgoing, int typeMod) {
        if (lead!=highbit) {
            if (next==null) {
                next=new CompactLongRecord2((byte)(highbit+1));
            }
            next.add(lead,id,outgoing,typeMod);
        } else {
            int value = (int)(id & 0x7FFFFFFF);
            if (!outgoing) value = ~value;
            counts[idx(outgoing)]++;
            values.add(value,typeMod);
        }
    }

    public long[][] getOutgoing() {
        return get(true);
    }

    public long[][] getIncoming() {
        return get(false);
    }

    private long[][] get(boolean outgoing) {
        long[][] result=new long[count(outgoing)][2];
        return collect(result,outgoing,0);
    }

    private long[][] collect(long[][] result, boolean positive, int offset) {
        final int max = counts[idx(positive)];
        int i=offset;
        for (int j = 0; j < values.count; j++) {
            int value = values.data[j][0];
            final boolean isPositive = value >= 0;
            if (isPositive == positive) {
                result[i][1] = values.data[j][1];
                result[i][0]= convert(value);
                i++;
            }
        }
        if (offset+max!=i) throw new IllegalStateException(String.format("Max %d != array counter %d",max,i));
        if (next!=null) return next.collect(result,positive,i);
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
