package org.neo4j.batchimport.csv;

import au.com.bytecode.opencsv.CSVReader;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Ignore;
import org.junit.Test;

import java.io.*;

import static org.neo4j.batchimport.csv.PerformanceTestFile.*;

/**
 * @author mh
 * @since 11.06.13
 */
@Ignore("Performance")
public class OpenCSVPerformanceTest {

    @Before
    public void setUp() throws Exception {
        createTestFileIfNeeded();
    }

    @Test
    public void testReadLineWithCommaSeparator() throws Exception {
        final BufferedReader reader = new BufferedReader(new FileReader(TEST_CSV));
        final CSVReader csvReader = new CSVReader(reader,'\t','"');

        int res = 0;
        long time = System.currentTimeMillis();
        String[] line = null;
        while ((line = csvReader.readNext()) != null) {
            res += line.length;
        }
        time = System.currentTimeMillis() - time;
        System.out.println("time = " + time + " ms.");
        Assert.assertEquals(ROWS * COLS, res);
    }
}
