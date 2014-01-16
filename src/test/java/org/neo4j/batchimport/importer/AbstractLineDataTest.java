package org.neo4j.batchimport.importer;

import org.junit.Test;

import java.io.StringReader;
import java.util.Collections;

import static org.junit.Assert.*;
import static org.junit.Assert.assertArrayEquals;
import static org.neo4j.helpers.collection.MapUtil.map;

/**
 * Created by mh on 26.08.13.
 */
public class AbstractLineDataTest {

    @Test
    public void testLabelNamedHeaderIsNotTreatedDifferently() throws Exception {
        StringReader reader = new StringReader("label\nfoo");
        CsvLineData data = new CsvLineData(reader, '\t', 0);
        assertTrue(data.readLine());
        assertEquals(Type.STRING,data.getHeader()[0].type);
        assertEquals("foo",data.getValue(0));
    }

    @Test
    public void testIdIsHandledCorrectly() throws Exception {
        StringReader reader = new StringReader("id:id\n123");
        CsvLineData data = new CsvLineData(reader, '\t', 0);
        assertTrue(data.processLine(""));
        assertEquals(Type.ID,data.getHeader()[0].type);
        assertEquals(123L,data.getValue(0));
        assertEquals(Collections.emptyMap(),data.getProperties());
    }

    @Test
    public void testLabelTypedHeaderHandledAsLabel() throws Exception {
        StringReader reader = new StringReader("label:label\nfoo");
        CsvLineData data = new CsvLineData(reader, '\t', 0);
        assertTrue(data.processLine(null));
        assertEquals(Type.LABEL, data.getHeader()[0].type);
        assertArrayEquals(new String[]{"foo"}, (Object[]) data.getValue(0));
        assertArrayEquals(new String[]{"foo"}, data.getTypeLabels());
    }
    @Test
    public void testFileWithLabelHasCorrectProperties() throws Exception {
        StringReader reader = new StringReader("prop\tlabel:label\nbar\tfoo");
        CsvLineData data = new CsvLineData(reader, '\t', 0);
        assertTrue(data.processLine(null));
        assertEquals(Type.LABEL, data.getHeader()[1].type);
        assertArrayEquals(new String[]{"foo"}, (Object[]) data.getValue(1));
        assertArrayEquals(new String[]{"foo"}, data.getTypeLabels());
        assertEquals(map("prop", "bar"), data.getProperties());
    }
}
