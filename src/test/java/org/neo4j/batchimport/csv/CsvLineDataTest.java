package org.neo4j.batchimport.csv;

import org.junit.Assert;
import org.junit.Test;
import org.neo4j.batchimport.importer.CsvLineData;

import java.io.StringReader;
import java.util.Map;

import static org.junit.Assert.assertEquals;

/**
 * @author mh
 * @since 29.11.12
 */
public class CsvLineDataTest {

    @Test
    public void testInvalidConversion() throws Exception {
        try {
            CsvLineData rowData = new CsvLineData(new StringReader("a\tb:int\tc\n2\tfoo\t3"), '\t', 0);
            rowData.updateMap();
            Assert.fail("Expected conversion exception");
        } catch(RuntimeException e) {
            assertEquals(true,e.getMessage().contains("row 1"));
            assertEquals(true,e.getMessage().contains("foo"));
            assertEquals(true,e.getMessage().contains("1. b"));
            assertEquals(true,e.getMessage().contains("type: INT"));
            assertEquals(true,e.getMessage().contains("NumberFormatException"));
        }
    }

    @Test
    public void testTrailingEmptyCells() throws Exception {
        CsvLineData rowData = new CsvLineData(new StringReader("a\tb\tc\n\t2\t3"), '\t', 0);
        final Map<String,Object> map = rowData.updateMap();
        assertEquals(null,map.get("a"));
        assertEquals("2",map.get("b"));
        assertEquals("3",map.get("c"));
    }
    @Test
    public void testLeadingAndTrailingEmptyCells() throws Exception {
        CsvLineData rowData = new CsvLineData(new StringReader("a\tb\tc\n\t2\t"), '\t', 0);
        final Map<String,Object> map = rowData.updateMap();
        assertEquals(null,map.get("a"));
        assertEquals("2",map.get("b"));
        assertEquals(null,map.get("c"));
    }
    @Test
    public void testLeadingEmptyCells() throws Exception {
        CsvLineData rowData = new CsvLineData(new StringReader("a\tb\tc\n1\t\t"), '\t', 0);
        final Map<String,Object> map = rowData.updateMap();
        assertEquals("1",map.get("a"));
        assertEquals(null,map.get("b"));
        assertEquals(null,map.get("c"));
    }
    @Test
    public void testEmptyRow() throws Exception {
        CsvLineData rowData = new CsvLineData(new StringReader("a\tb\tc\n"), '\t', 0);
        final Map<String,Object> map = rowData.updateMap();
        assertEquals(null,map.get("a"));
        assertEquals(null,map.get("b"));
        assertEquals(null,map.get("c"));
    }

    @Test
    public void testLeadOneRow() throws Exception {
        CsvLineData rowData = new CsvLineData(new StringReader("a\tb\tc\n1\t"), '\t', 0);
        final Map<String,Object> map = rowData.updateMap();
        assertEquals("1",map.get("a"));
        assertEquals(null,map.get("b"));
        assertEquals(null,map.get("c"));
    }
    @Test
    public void testLeadTwoRow() throws Exception {
        CsvLineData rowData = new CsvLineData(new StringReader("a\tb\tc\n1\t2"), '\t', 0);

        final Map<String,Object> map = rowData.updateMap();
        assertEquals("1",map.get("a"));
        assertEquals("2",map.get("b"));
        assertEquals(null,map.get("c"));
    }
    @Test
    public void testNormalCells() throws Exception {
        CsvLineData rowData = new CsvLineData(new StringReader("a\tb\tc\n1\t2\t3"), '\t', 0);
        final Map<String,Object> map = rowData.updateMap();
        assertEquals("1",map.get("a"));
        assertEquals("2",map.get("b"));
        assertEquals("3",map.get("c"));
    }

    @Test
    public void testQuotedHeader() throws Exception {
        CsvLineData rowData = new CsvLineData(new StringReader("\"a\"\tb\tc\n1\t2\t3"), '\t', 0);
        final Map<String,Object> map = rowData.updateMap();
        assertEquals("1",map.get("a"));
        assertEquals("2",map.get("b"));
        assertEquals("3",map.get("c"));
    }
    @Test
    public void testQuotedValue() throws Exception {
        CsvLineData rowData = new CsvLineData(new StringReader("\"a\"\tb\tc\n\"1\"\t2\t3"), '\t', 0);
        final Map<String,Object> map = rowData.updateMap();
        assertEquals("1",map.get("a"));
        assertEquals("2",map.get("b"));
        assertEquals("3",map.get("c"));
    }

    @Test
    public void testQuotedValueWithNewline() throws Exception {
        CsvLineData rowData = new CsvLineData(new StringReader("\"a\"\tb\tc\n\"1\n2\"\t2\t3"), '\t', 0);
        final Map<String,Object> map = rowData.updateMap();
        assertEquals("1\n2",map.get("a"));
        assertEquals("2",map.get("b"));
        assertEquals("3",map.get("c"));
    }

    @Test
    public void testQuotedValueWithNewlineAndCommas() throws Exception {
        CsvLineData rowData = new CsvLineData(new StringReader("\"a\",b,c\n\"1\n2\",2,3"), ',', 0);
        final Map<String,Object> map = rowData.updateMap();
        assertEquals("1\n2",map.get("a"));
        assertEquals("2",map.get("b"));
        assertEquals("3",map.get("c"));
    }

    @Test
    public void testConvert() throws Exception {
        CsvLineData rowData = new CsvLineData(new StringReader("a:int\tb:float\tc:boolean"+"\n"+"1\t2.1\ttrue"), '\t', 0);
        final Map<String,Object> map = rowData.updateMap();
        assertEquals(1,map.get("a"));
        assertEquals(2.1F,map.get("b"));
        assertEquals(true,map.get("c"));
    }
}
