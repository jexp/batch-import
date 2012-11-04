package org.neo4j.batchimport.collections;

import org.junit.Test;

import java.util.Random;

import static org.junit.Assert.assertEquals;

/**
 * @author mh
 * @since 03.11.12
 */
public class CompactLongRecordTest {

    private static final int COUNT = 10000000;
    private static final int LONGS = 1000;
    private final CompactLongRecord record = new CompactLongRecord((byte)0);

    @Test
    public void testAddIntValues() throws Exception {
        record.add(0, true);
        record.add(Integer.MAX_VALUE, true);
        long[] ids = record.getOutgoing();
        assertEquals(2,record.count(true));
        assertEquals(0,ids[0]);
        assertEquals(Integer.MAX_VALUE,ids[1]);
    }

    @Test
    public void testAddIncomingValues() throws Exception {
        record.add(0, false);
        record.add(Integer.MAX_VALUE, false);
        assertEquals(2,record.count(false));
        long[] ids = record.getIncoming();
        assertEquals(0,ids[0]);
        assertEquals(Integer.MAX_VALUE,ids[1]);
    }

    @Test
    public void testAddLongValues() throws Exception {
        record.add(1L << 32, true);
        record.add(1L << 33, true);
        long[] ids = record.getOutgoing();
        assertEquals(2,record.count(true));
        assertEquals(1L << 32,ids[0]);
        assertEquals(1L << 33,ids[1]);
    }

    @Test
    public void testPerformance() throws Exception {
        long[] input = generateLongs(LONGS);

        long free = Runtime.getRuntime().freeMemory();
        long time = System.currentTimeMillis();

        for (int i=0;i< COUNT;i++) {
            record.add(input[i % LONGS],true);
        }

        final long usedMemory = free - Runtime.getRuntime().freeMemory();
        System.out.println("took "+(System.currentTimeMillis()-time)+ " ms, used "+ usedMemory / 1024/1024+ " MB");
        assertEquals(COUNT,record.count(true));
    }

    private long[] generateLongs(int count) {
        final Random rnd = new Random();
        final long[] result = new long[count];
        for (int i = 0; i < count; i++) {
            result[i] = rnd.nextLong() & 0x7FFFFFFFFL;
        }
        return result;
    }
}
