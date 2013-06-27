package org.neo4j.batchimport.csv;

import org.junit.Test;
import org.neo4j.batchimport.importer.RowData;

import java.util.Map;

import static org.junit.Assert.assertEquals;

/**
 * @author mh
 * @since 29.11.12
 */
public class RowDataTest {

    private final RowData rowData = new RowData("a\tb\tc", "\t", 0);

    @Test
    public void testTrailingEmptyCells() throws Exception {
        final Map<String,Object> map = rowData.updateMap("\t2\t3");
        assertEquals(null,map.get("a"));
        assertEquals("2",map.get("b"));
        assertEquals("3",map.get("c"));
    }
    @Test
    public void testLeadingAndTrailingEmptyCells() throws Exception {
        final Map<String,Object> map = rowData.updateMap("\t2\t");
        assertEquals(null,map.get("a"));
        assertEquals("2",map.get("b"));
        assertEquals(null,map.get("c"));
    }
    @Test
    public void testLeadingEmptyCells() throws Exception {
        final Map<String,Object> map = rowData.updateMap("1\t\t");
        assertEquals("1",map.get("a"));
        assertEquals(null,map.get("b"));
        assertEquals(null,map.get("c"));
    }
    @Test
    public void testEmptyRow() throws Exception {
        final Map<String,Object> map = rowData.updateMap("");
        assertEquals(null,map.get("a"));
        assertEquals(null,map.get("b"));
        assertEquals(null,map.get("c"));
    }

    @Test
    public void testLeadOneRow() throws Exception {
        final Map<String,Object> map = rowData.updateMap("1\t");
        assertEquals("1",map.get("a"));
        assertEquals(null,map.get("b"));
        assertEquals(null,map.get("c"));
    }
    @Test
    public void testLeadTwoRow() throws Exception {
        final Map<String,Object> map = rowData.updateMap("1\t2");
        assertEquals("1",map.get("a"));
        assertEquals("2",map.get("b"));
        assertEquals(null,map.get("c"));
    }
    @Test
    public void testNormalCells() throws Exception {
        final Map<String,Object> map = rowData.updateMap("1\t2\t3");
        assertEquals("1",map.get("a"));
        assertEquals("2",map.get("b"));
        assertEquals("3",map.get("c"));
    }

    @Test
    public void testConvert() throws Exception {
        final RowData rowData = new RowData("a:int\tb:float\tc:boolean", "\t", 0);
        final Map<String,Object> map = rowData.updateMap("1\t2.1\ttrue");
        assertEquals(1,map.get("a"));
        assertEquals(2.1F,map.get("b"));
        assertEquals(true,map.get("c"));
    }
}
