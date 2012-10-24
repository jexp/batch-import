import org.junit.Test;
import org.neo4j.batchimport.Importer;

import java.util.Map;

import static org.junit.Assert.assertEquals;

public class DataTest {
    @Test
    public void testConvertType() throws Exception {
        Importer.Data data = new Importer.Data("a:int\tb:float\tc:float", "\t", 0);
        Map<String,Object> row = data.update("100\t100.0\t1E+10");
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
        Importer.Data data = new Importer.Data("start\tend\ttype\tproperty", "\t", 3);
        Object[] rel = new Object[3];
        Map<String,Object> row = data.update("1\t2\tTYPE\tPROPERTY", rel);
        assertEquals("1", rel[0]);
        assertEquals("2", rel[1]);
        assertEquals("TYPE", rel[2]);
        assertEquals("PROPERTY", row.get("property"));
    }

    @Test
    public void testRelationshipWithNoProperty() throws Exception {
        Importer.Data data = new Importer.Data("start\tend\ttype", "\t", 3);
        Object[] rel = new Object[3];
        Map<String,Object> row = data.update("1\t2\tTYPE", rel);
        assertEquals("1", rel[0]);
        assertEquals("2", rel[1]);
        assertEquals("TYPE", rel[2]);
    }
}
