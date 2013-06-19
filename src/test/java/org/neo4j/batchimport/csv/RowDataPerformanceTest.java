package org.neo4j.batchimport.csv;

import au.com.bytecode.opencsv.CSVReader;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Ignore;
import org.junit.Test;
import org.neo4j.batchimport.importer.RowData;
import static org.neo4j.batchimport.csv.PerformanceTestFile.*;

import java.io.*;

/**
 * @author mh
 * @since 11.06.13
 */
@Ignore("Performance")
public class RowDataPerformanceTest {
    @Before
    public void setUp() throws Exception {
        createTestFileIfNeeded();
    }

    @Test
    public void testPerformance() throws Exception {
        final BufferedReader reader = new BufferedReader(new FileReader(TEST_CSV));
        final RowData rowData = new RowData(reader.readLine(), "\t", 0);

        int res = 0;
        long time = System.currentTimeMillis();
        String line;
        while ((line = reader.readLine()) != null) {
            rowData.processLine(line);
            res += rowData.getColumnCount();
        }
        time = System.currentTimeMillis() - time;
        System.out.println("time = " + time + " ms.");
        Assert.assertEquals((ROWS-1) * COLS, res);
    }
}
