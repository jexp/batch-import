package org.neo4j.batchimport.csv;

import static org.junit.Assert.assertEquals;

import java.io.StringReader;
import java.util.HashMap;
import java.util.Map;

import org.junit.Before;
import org.junit.Test;
import org.neo4j.batchimport.importer.ChunkerLineData;
import org.neo4j.batchimport.utils.Config;

/**
 * @author mh
 * @since 29.11.12
 */
public class ChunkerRowDataTest {

	private Config config;

	@Before
	public void setup() {
		Map<String, String> configData = new HashMap<String, String>();
		config = new Config(configData);
	}

	@Test
	public void testTrailingEmptyCells() throws Exception {
		ChunkerLineData rowData = new ChunkerLineData(new StringReader(
				"a\tb\tc\n\t2\t3"), '\t', 0, config);
		final Map<String, Object> map = rowData.updateMap();
		assertEquals(null, map.get("a"));
		assertEquals("2", map.get("b"));
		assertEquals("3", map.get("c"));
	}

	@Test
	public void testLeadingAndTrailingEmptyCells() throws Exception {
		ChunkerLineData rowData = new ChunkerLineData(new StringReader(
				"a\tb\tc\n\t2\t"), '\t', 0, config);
		final Map<String, Object> map = rowData.updateMap();
		assertEquals(null, map.get("a"));
		assertEquals("2", map.get("b"));
		assertEquals(null, map.get("c"));
	}

	@Test
	public void testLeadingEmptyCells() throws Exception {
		ChunkerLineData rowData = new ChunkerLineData(new StringReader(
				"a\tb\tc\n1\t\t"), '\t', 0, config);
		final Map<String, Object> map = rowData.updateMap();
		assertEquals("1", map.get("a"));
		assertEquals(null, map.get("b"));
		assertEquals(null, map.get("c"));
	}

	@Test
	public void testEmptyRow() throws Exception {
		ChunkerLineData rowData = new ChunkerLineData(new StringReader(
				"a\tb\tc\n"), '\t', 0, config);
		final Map<String, Object> map = rowData.updateMap();
		assertEquals(null, map.get("a"));
		assertEquals(null, map.get("b"));
		assertEquals(null, map.get("c"));
	}

	@Test
	public void testLeadOneRow() throws Exception {
		ChunkerLineData rowData = new ChunkerLineData(new StringReader(
				"a\tb\tc\n1\t"), '\t', 0, config);
		final Map<String, Object> map = rowData.updateMap();
		assertEquals("1", map.get("a"));
		assertEquals(null, map.get("b"));
		assertEquals(null, map.get("c"));
	}

	@Test
	public void testLeadTwoRow() throws Exception {
		ChunkerLineData rowData = new ChunkerLineData(new StringReader(
				"a\tb\tc\n1\t2"), '\t', 0, config);

		final Map<String, Object> map = rowData.updateMap();
		assertEquals("1", map.get("a"));
		assertEquals("2", map.get("b"));
		assertEquals(null, map.get("c"));
	}

	@Test
	public void testNormalCells() throws Exception {
		ChunkerLineData rowData = new ChunkerLineData(new StringReader(
				"a\tb\tc\n1\t2\t3"), '\t', 0, config);
		final Map<String, Object> map = rowData.updateMap();
		assertEquals("1", map.get("a"));
		assertEquals("2", map.get("b"));
		assertEquals("3", map.get("c"));
	}

	@Test
	public void testHandleNewLines() throws Exception {
		ChunkerLineData rowData = new ChunkerLineData(new StringReader(
				"a,b,c:int\r\n1,2,3\r\n4,5,6"), ',', 0, config);
		Map<String, Object> map = rowData.updateMap();
		assertEquals("1", map.get("a"));
		assertEquals("2", map.get("b"));
		assertEquals(3, map.get("c"));
		map = rowData.updateMap();
		assertEquals("4", map.get("a"));
		assertEquals("5", map.get("b"));
		assertEquals(6, map.get("c"));
	}

	@Test
	public void testNormalWithCommas() throws Exception {
		ChunkerLineData rowData = new ChunkerLineData(new StringReader(
				"a,b,c\n1,2,3"), ',', 0, config);
		final Map<String, Object> map = rowData.updateMap();
		assertEquals("1", map.get("a"));
		assertEquals("2", map.get("b"));
		assertEquals("3", map.get("c"));
	}

	@Test
	public void testNormalCellsTwoRows() throws Exception {
		ChunkerLineData rowData = new ChunkerLineData(new StringReader(
				"a\tb\tc\n1\t2\t3\n4\t5\t6"), '\t', 0, config);
		final Map<String, Object> row1 = rowData.updateMap();
		assertEquals("1", row1.get("a"));
		assertEquals("2", row1.get("b"));
		assertEquals("3", row1.get("c"));
		final Map<String, Object> row2 = rowData.updateMap();
		assertEquals("4", row2.get("a"));
		assertEquals("5", row2.get("b"));
		assertEquals("6", row2.get("c"));
	}

	@Test
	public void testConvert() throws Exception {
		ChunkerLineData rowData = new ChunkerLineData(new StringReader(
				"a:int\tb:float\tc:boolean" + "\n" + "1\t2.1\ttrue"), '\t', 0, config);
		final Map<String, Object> map = rowData.updateMap();
		assertEquals(1, map.get("a"));
		assertEquals(2.1F, map.get("b"));
		assertEquals(true, map.get("c"));
	}
}
