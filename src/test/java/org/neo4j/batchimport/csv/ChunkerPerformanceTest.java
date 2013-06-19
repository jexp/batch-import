package org.neo4j.batchimport.csv;

import org.junit.Assert;
import org.junit.Before;
import org.junit.Ignore;
import org.junit.Test;
import org.neo4j.batchimport.importer.RowData;
import org.neo4j.batchimport.utils.Chunker;

import java.io.*;

/**
 * @author mh
 * @since 11.06.13
 */
@Ignore("Performance")
public class ChunkerPerformanceTest {

    @Before
    public void setUp() throws Exception {
        PerformanceTestFile.createTestFileIfNeeded();
    }

    @Test
    public void testPerformance() throws Exception {
        final BufferedReader reader = new BufferedReader(new FileReader(PerformanceTestFile.TEST_CSV));
        final Chunker chunker = new Chunker(reader, '\t');

        int res = 0;
        long time = System.currentTimeMillis();
        String token;
        while ( (token = chunker.nextWord()) != Chunker.EOF)  {
            if (token!=Chunker.NO_VALUE && token != Chunker.EOL) res++;
        }
        time = System.currentTimeMillis() - time;
        System.out.println("time = " + time + " ms.");
        Assert.assertEquals((PerformanceTestFile.ROWS) * PerformanceTestFile.COLS, res);
    }

}
