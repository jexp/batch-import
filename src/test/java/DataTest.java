import org.junit.Assert;
import org.junit.Test;
import org.neo4j.batchimport.importer.RowData;

import java.util.Arrays;
import java.util.Map;

import static org.junit.Assert.assertArrayEquals;
import static org.junit.Assert.assertEquals;

public class DataTest {
    @Test
    public void testConvertType() throws Exception {
        RowData data = new RowData("a:int\tb:float\tc:float", "\t", 0);
        data.processLine("100\t100.0\t1E+10");
        Map<String,Object> row = data.getProperties();
        assertEquals(100, row.get("a"));
        assertEquals(true,row.get("b") instanceof Float);
        assertEquals(100.0F, row.get("b"));
        assertEquals(true,row.get("b") instanceof Float);
        assertEquals(100.0F, row.get("b"));
        assertEquals(true,row.get("c") instanceof Float);
        assertEquals(1E+10F, row.get("c"));
    }

    @Test
    public void testRelationship() throws Exception {
        RowData data = new RowData("start\tend\ttype\tproperty", "\t", 3);
        data.processLine("1\t2\tTYPE\tPROPERTY");
        Map<String,Object> row = data.getProperties();
        assertEquals("1", data.getValue(0));
        assertEquals("2", data.getValue(1));
        assertEquals("TYPE", data.getTypeLabels()[0]);
        assertEquals("PROPERTY", row.get("property"));
    }

    @Test
    public void testRelationshipWithNoProperty() throws Exception {
        RowData data = new RowData("start\tend\ttype", "\t", 3);
        data.processLine("1\t2\tTYPE");
        assertEquals("1", data.getValue(0));
        assertEquals("2", data.getValue(1));
        assertEquals("TYPE", data.getTypeLabels()[0]);
    }

    @Test
    public void testNodeLabels() throws Exception {
        RowData data = new RowData("labels", "\t", 3);
        data.processLine("TYPE1,TYPE2");
        assertEquals("TYPE1", data.getTypeLabels()[0]);
        assertEquals("TYPE2", data.getTypeLabels()[1]);
    }
    @Test
    public void testNodeLabelsWithLabelType() throws Exception {
        RowData data = new RowData("foo:label", "\t", 3);
        data.processLine("TYPE1,TYPE2");
        assertEquals("TYPE1", data.getTypeLabels()[0]);
        assertEquals("TYPE2", data.getTypeLabels()[1]);
    }
    @Test
    public void testArrayType() throws Exception {
        RowData data = new RowData("a:int\tb:float\tc:string_array", "\t", 0);
        data.processLine("100\t100.0\tbagels,coffee,tea");
        Map<String,Object> row = data.getProperties();
        assertEquals(100, row.get("a"));
        assertEquals(true,row.get("b") instanceof Float);
        assertEquals(100.0F, row.get("b"));
        assertEquals(true,row.get("b") instanceof Float);
        assertEquals(100.0F, row.get("b"));
        assertEquals(true,row.get("c") instanceof String[]);
        assertArrayEquals(new String[]{"bagels", "coffee", "tea"}, (String[]) row.get("c"));
    }

    @Test
    public void testBooleanArrayType() throws Exception {
        RowData data = new RowData("a:int\tb:float\tc:boolean_array", "\t", 0);
        data.processLine("100\t100.0\ttrue,false,true");
        Map<String,Object> row = data.getProperties();
        assertEquals(100, row.get("a"));
        assertEquals(true,row.get("b") instanceof Float);
        assertEquals(100.0F, row.get("b"));
        assertEquals(true,row.get("b") instanceof Float);
        assertEquals(100.0F, row.get("b"));
        assertEquals(true,row.get("c") instanceof boolean[]);
        Assert.assertTrue(Arrays.equals(new boolean[]{true, false, true}, (boolean[]) row.get("c")));
    }
    @Test
    public void testIntArrayType() throws Exception {
        RowData data = new RowData("a:int\tb:float\tc:int_array", "\t", 0);
        data.processLine("100\t100.0\t1,2,3");
        Map<String,Object> row = data.getProperties();
        assertEquals(100, row.get("a"));
        assertEquals(true,row.get("b") instanceof Float);
        assertEquals(100.0F, row.get("b"));
        assertEquals(true,row.get("b") instanceof Float);
        assertEquals(100.0F, row.get("b"));
        assertEquals(true,row.get("c") instanceof int[]);
        assertArrayEquals(new int[] {1,2,3}, (int[])row.get("c"));
    }
}
