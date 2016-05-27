package org.neo4j.batchimport.utils;

import org.junit.Test;
import org.neo4j.helpers.collection.Iterators;

import java.io.FileWriter;
import java.io.IOException;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;
import static org.neo4j.batchimport.utils.FileIterator.DELIM;

/**
 * @author Michael Hunger @since 04.11.13
 */
public class FileIteratorTest {

    public static final int LINES = 10;
    public static final int LINES_1M = 1_000_000;

    @Test
    public void testReadLines() throws Exception {
        String fileName = writeFile(LINES);

        int count = 0;
        FileIterator it = new FileIterator(fileName);
        while (it.hasNext()) {
            FileIterator.Line line = it.next();
            assertEquals(count, line.lineNo);
            assertEquals(""+start(count)+ DELIM+end(count)+ DELIM, line.line);
            count++;
        }
        assertEquals(false,it.hasNext());
        assertEquals(LINES, count);
    }

    private String writeFile(int lines) throws IOException {
        String fileName = "target/FileIteratorTest.txt";
        FileWriter writer = new FileWriter(fileName);
        for (int i=0;i< lines;i++) {
            writer.write(String.format("%d%s%d%s%n", start(i),DELIM, end(i),DELIM));
        }
        writer.close();
        return fileName;
    }

    private int start(int i) {
        return i;
    }

    private int end(int i) {
        return i+10-i%20;
    }

    @Test
    public void testPerformance() throws Exception {
        String file = writeFile(LINES_1M);
        FileIterator reader = new FileIterator(file);
        long time = System.currentTimeMillis();
        int count = (int) Iterators.count(reader);
        long delta = System.currentTimeMillis() - time;
        System.out.println("delta = " + delta);
        assertTrue("timeout "+delta+" > 1000 ms", delta < 1000);
        assertEquals(LINES_1M, count);
        reader.close();

    }
}
