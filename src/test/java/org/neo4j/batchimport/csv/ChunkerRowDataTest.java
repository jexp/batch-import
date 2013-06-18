package org.neo4j.batchimport.csv;

import org.junit.Test;
import org.neo4j.batchimport.importer.ChunkerRowData;

import java.io.StringReader;
import java.util.Map;

import static org.junit.Assert.assertEquals;

/**
 * @author mh
 * @since 29.11.12
 */
public class ChunkerRowDataTest {

    private final ChunkerRowData rowData = new ChunkerRowData(new StringReader("a\tb\tc"), '\t', 0);

    @Test
    public void testTrailingEmptyCells() throws Exception {
        ChunkerRowData rowData = new ChunkerRowData(new StringReader("a\tb\tc\n\t2\t3"), '\t', 0);
        final Map<String,Object> map = rowData.updateMap();
        assertEquals(null,map.get("a"));
        assertEquals("2",map.get("b"));
        assertEquals("3",map.get("c"));
    }
    @Test
    public void testLeadingAndTrailingEmptyCells() throws Exception {
        ChunkerRowData rowData = new ChunkerRowData(new StringReader("a\tb\tc\n\t2\t"), '\t', 0);
        final Map<String,Object> map = rowData.updateMap();
        assertEquals(null,map.get("a"));
        assertEquals("2",map.get("b"));
        assertEquals(null,map.get("c"));
    }
    @Test
    public void testLeadingEmptyCells() throws Exception {
        ChunkerRowData rowData = new ChunkerRowData(new StringReader("a\tb\tc\n1\t\t"), '\t', 0);
        final Map<String,Object> map = rowData.updateMap();
        assertEquals("1",map.get("a"));
        assertEquals(null,map.get("b"));
        assertEquals(null,map.get("c"));
    }
    @Test
    public void testEmptyRow() throws Exception {
        ChunkerRowData rowData = new ChunkerRowData(new StringReader("a\tb\tc\n"), '\t', 0);
        final Map<String,Object> map = rowData.updateMap();
        assertEquals(null,map.get("a"));
        assertEquals(null,map.get("b"));
        assertEquals(null,map.get("c"));
    }

    @Test
    public void testLeadOneRow() throws Exception {
        ChunkerRowData rowData = new ChunkerRowData(new StringReader("a\tb\tc\n1\t"), '\t', 0);
        final Map<String,Object> map = rowData.updateMap();
        assertEquals("1",map.get("a"));
        assertEquals(null,map.get("b"));
        assertEquals(null,map.get("c"));
    }
    @Test
    public void testLeadTwoRow() throws Exception {
        ChunkerRowData rowData = new ChunkerRowData(new StringReader("a\tb\tc\n1\t2"), '\t', 0);

        final Map<String,Object> map = rowData.updateMap();
        assertEquals("1",map.get("a"));
        assertEquals("2",map.get("b"));
        assertEquals(null,map.get("c"));
    }
    @Test
    public void testNormalCells() throws Exception {
        ChunkerRowData rowData = new ChunkerRowData(new StringReader("a\tb\tc\n1\t2\t3"), '\t', 0);
        final Map<String,Object> map = rowData.updateMap();
        assertEquals("1",map.get("a"));
        assertEquals("2",map.get("b"));
        assertEquals("3",map.get("c"));
    }

    @Test
    public void testConvert() throws Exception {
        ChunkerRowData rowData = new ChunkerRowData(new StringReader("a:int\tb:float\tc:boolean"+"\n"+"1\t2.1\ttrue"), '\t', 0);
        final Map<String,Object> map = rowData.updateMap();
        assertEquals(1,map.get("a"));
        assertEquals(2.1F,map.get("b"));
        assertEquals(true,map.get("c"));
    }
}
