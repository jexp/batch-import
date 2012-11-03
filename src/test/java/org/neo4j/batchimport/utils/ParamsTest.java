package org.neo4j.batchimport.utils;

import org.junit.Test;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

/**
 * @author mh
 * @since 02.11.12
 */
public class ParamsTest {
    Params params = new Params("foo bar","file","42");

    @Test
    public void testInvalid() throws Exception {
        assertEquals(false,params.invalid());
        assertEquals(true,new Params("foo bar", "file").invalid());
    }

    @Test
    public void testLength() throws Exception {
        assertEquals(2,params.length());
    }

    @Test
    public void testToString() throws Exception {
        assertEquals("foo bar",params.toString());
    }

    @Test
    public void testFile() throws Exception {
        assertEquals("file",params.file("foo").getPath());
    }

    @Test
    public void testLongValue() throws Exception {
        assertEquals(42L,params.longValue("bar"));
    }
    @Test
    public void testIntValue() throws Exception {
        assertEquals(42,params.intValue("bar"));
    }
}
