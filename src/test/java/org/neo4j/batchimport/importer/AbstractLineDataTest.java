package org.neo4j.batchimport.importer;

import static org.junit.Assert.assertArrayEquals;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;
import static org.neo4j.helpers.collection.MapUtil.map;

import java.io.StringReader;
import java.util.Collections;
import java.util.HashMap;
import java.util.Map;

import org.junit.Before;
import org.junit.Test;
import org.neo4j.batchimport.utils.Config;

/**
 * Created by mh on 26.08.13.
 */
public class AbstractLineDataTest {
	private Config config;

	@Before
	public void setup() {
		Map<String, String> configData = new HashMap<String, String>();
		config = new Config(configData);
		config.setArraysSeparator("\t");
	}
    @Test
    public void testLabelNamedHeaderIsNotTreatedDifferently() throws Exception {
        StringReader reader = new StringReader("label\nfoo");
        CsvLineData data = new CsvLineData(reader, '\t', 0, config);
        assertTrue(data.readLine());
        assertEquals(Type.STRING,data.getHeader()[0].type);
        assertEquals("foo",data.getValue(0));
    }

    @Test
    public void testIdIsHandledCorrectly() throws Exception {
        StringReader reader = new StringReader("id:id\n123");
        CsvLineData data = new CsvLineData(reader, '\t', 0, config);
        assertTrue(data.processLine(""));
        assertEquals(Type.ID,data.getHeader()[0].type);
        assertEquals(123L,data.getValue(0));
        assertEquals(Collections.emptyMap(),data.getProperties());
    }

    @Test
    public void testLabelTypedHeaderHandledAsLabel() throws Exception {
        StringReader reader = new StringReader("label:label\nfoo");
        CsvLineData data = new CsvLineData(reader, '\t', 0, config);
        assertTrue(data.processLine(null));
        assertEquals(Type.LABEL, data.getHeader()[0].type);
        assertArrayEquals(new String[]{"foo"}, (Object[]) data.getValue(0));
        assertArrayEquals(new String[]{"foo"}, data.getTypeLabels());
    }
    @Test
    public void testFileWithLabelHasCorrectProperties() throws Exception {
        StringReader reader = new StringReader("prop\tlabel:label\nbar\tfoo");
        CsvLineData data = new CsvLineData(reader, '\t', 0, config);
        assertTrue(data.processLine(null));
        assertEquals(Type.LABEL, data.getHeader()[1].type);
        assertArrayEquals(new String[]{"foo"}, (Object[]) data.getValue(1));
        assertArrayEquals(new String[]{"foo"}, data.getTypeLabels());
        assertEquals(map("prop", "bar"), data.getProperties());
    }
}
