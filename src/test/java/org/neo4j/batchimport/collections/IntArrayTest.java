package org.neo4j.batchimport.collections;

import org.junit.Test;

import static org.junit.Assert.assertEquals;

/**
 * @author mh
 * @since 03.11.12
 */
public class IntArrayTest {

    private final IntArray array = new IntArray();

    @Test
    public void testAdd() throws Exception {
        array.add(42);
        assertEquals(1, array.count);
        assertEquals(42, array.data[0]);
    }

    @Test
    public void testEmptyArray() throws Exception {
        assertEquals(false,array.hasData());
        assertEquals(false,array.hasPositive());
        assertEquals(false,array.hasNegative());
    }

    @Test
    public void testHasPositive() throws Exception {
        array.add(42);
        assertEquals(true,array.hasData());
        assertEquals(true,array.hasPositive());
        assertEquals(false,array.hasNegative());
    }
    @Test
    public void testHasNegative() throws Exception {
        array.add(-1);
        assertEquals(true,array.hasData());
        assertEquals(false,array.hasPositive());
        assertEquals(true, array.hasNegative());
    }

    @Test
    public void testPositiveLast() throws Exception {
        array.add(-1);
        array.add(42);
        assertEquals(2, array.count);
        assertEquals(true, array.hasPositive());
        assertEquals(true, array.hasNegative());
        assertEquals(42, array.firstPositive());
        assertEquals(-1, array.firstNegative());
    }

    @Test
    public void testPositiveFirst() throws Exception {
        array.add(42);
        array.add(-1);
        assertEquals(2, array.count);
        assertEquals(42, array.firstPositive());
        assertEquals(-1, array.firstNegative());
    }
}
