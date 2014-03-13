package org.neo4j.batchimport.csv;

import static org.neo4j.batchimport.csv.PerformanceTestFile.COLS;
import static org.neo4j.batchimport.csv.PerformanceTestFile.ROWS;
import static org.neo4j.batchimport.csv.PerformanceTestFile.TEST_CSV;
import static org.neo4j.batchimport.csv.PerformanceTestFile.createTestFileIfNeeded;

import java.io.BufferedReader;
import java.io.FileReader;

import org.junit.Assert;
import org.junit.Before;
import org.junit.Ignore;
import org.junit.Test;
import org.neo4j.batchimport.importer.RowData;
import org.neo4j.batchimport.utils.Config;

/**
 * @author mh
 * @since 11.06.13
 */
@Ignore("Performance")
public class RowDataPerformanceTest {
	private Config config;

	@Before
	public void setUp() throws Exception {
		createTestFileIfNeeded();
		config = new Config(null);
	}

	@Test
	public void testPerformance() throws Exception {
		final BufferedReader reader = new BufferedReader(new FileReader(
				TEST_CSV));
		String headerLine = reader.readLine();
		Assert.assertNotNull(headerLine);
		final RowData rowData = new RowData(headerLine, "\t", 0, config);

		int res = 0;
		long time = System.currentTimeMillis();
		String line;
		while ((line = reader.readLine()) != null) {
			rowData.processLine(line);
			res += rowData.getColumnCount();
		}
		reader.close();
		time = System.currentTimeMillis() - time;
		System.out.println("time = " + time + " ms.");
		Assert.assertEquals((ROWS - 1) * COLS, res);
	}
}
