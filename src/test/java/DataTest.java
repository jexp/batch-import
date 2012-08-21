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
}
