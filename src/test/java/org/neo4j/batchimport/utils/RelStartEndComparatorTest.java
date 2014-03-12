package org.neo4j.batchimport.utils;

import org.junit.Assert;
import org.junit.Test;

import static org.junit.Assert.assertEquals;
import static org.neo4j.batchimport.utils.FileIterator.Line.from;

/**
 * @author Michael Hunger @since 04.11.13
 */
public class RelStartEndComparatorTest {

    private final FileIterator.RelStartEndComparator comparator = new FileIterator.RelStartEndComparator();

    @Test
    public void testCompareMinStartEnd() throws Exception {
        Assert.assertEquals(-1, comparator.compare(from(1, "1\t2\t"), from(2, "1\t2\t")));
        Assert.assertEquals(1, comparator.compare(from(2, "1\t2\t"), from(1, "1\t2\t")));

        assertEquals(0, "1\t2\t", "1\t2\t");
        assertEquals(0, "2\t1\t", "1\t2\t");
        assertEquals(-1,"2\t1\t", "1\t3\t");
        assertEquals(-1, "1\t2\t", "3\t4\t");
        assertEquals(1, "3\t1\t", "1\t2\t");
        assertEquals(1, "3\t4\t", "1\t2\t");
    }

    private void assertEquals(int expected, String line1, String line2) {
        Assert.assertEquals(expected, comparator.compare(from(1, line1), from(1, line2)));
    }
}
