package org.neo4j.batchimport.utils;

import org.junit.Test;
import org.neo4j.helpers.collection.Iterators;

import java.io.BufferedReader;
import java.io.FileReader;
import java.io.FileWriter;

import static org.junit.Assert.assertEquals;
import static org.neo4j.batchimport.utils.FileIterator.DELIM;

/**
 * @author Michael Hunger @since 04.11.13
 */
//@Ignore("Doesn't work because 'equal' lines are squashed in mapdb, idea add line # as last criterium")
public class RelationshipSorterTest {
    private static final int LINES = 10;

    @Test
    public void testSortRelationshipFile() throws Exception {
        String fileName = "target/RelationshipSorterTest.txt";
        String targetFile = fileName + "_sorted";

        int written=1;
        FileWriter writer = new FileWriter(fileName);
        writer.write("" +"start" + DELIM + "end" + DELIM + '\n');
        for (int i = LINES - 1; i >= 0; i--) {
            for (int j = LINES - 1; j >= 0; j--) {
                String line = "" +i + DELIM + j + DELIM + '\n';
                writer.write(line);
                written++;
            }
        }
        writer.close();
        assertEquals(written, Iterators.count(new FileIterator(fileName)));

        RelationshipSorter.main(fileName, targetFile);
        BufferedReader reader = new BufferedReader(new FileReader(targetFile));
        String line = null;
        String[] last = null;
        int count = 1;
        reader.readLine();
        while ((line = reader.readLine()) != null) {
            String[] parts = line.split("\t");
            if (last!=null) {
                String msg = String.format("last min(%s,%s) < curr min(%s,%s)", last[0], last[1], parts[0], parts[1]);
                assertEquals(msg, true, Math.min(Integer.parseInt(last[0]),Integer.parseInt(last[1])) <= Math.min(Integer.parseInt(parts[0]),Integer.parseInt(parts[1])));
            }
            last = parts;
            count++;
        }
        assertEquals(written,count);
    }
}
