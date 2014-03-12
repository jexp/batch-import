package org.neo4j.batchimport.utils;

import org.mapdb.Pump;
import org.neo4j.helpers.collection.IteratorWrapper;

import java.io.BufferedWriter;
import java.io.FileWriter;
import java.io.IOException;
import java.util.Iterator;

/**
 * @author Michael Hunger @since 04.11.13
 */
public class RelationshipSorter {

    public static final int BUFFER = 1024 * 1024;

    public static void main(String... args) throws IOException {
        System.err.println("Usage mvn exec:java -Dexec.mainClass=org.neo4j.batchimport.utils.RelationshipSorter -Dexec.args='rels.csv rels_sorted.csv'");
        final String file = args[0];
        String file2 = args[1];
        long time = System.currentTimeMillis();
        FileIterator reader0 = new FileIterator(file);
        Iterator<FileIterator.Line> reader = wrapStatistics(reader0);
        FileIterator.Line header = reader.next();
        Iterator<FileIterator.Line> result = Pump.sort(reader, 1_000_000, new FileIterator.RelStartEndComparator(), new FileIterator.LineSerializer());
        BufferedWriter writer = new BufferedWriter(new FileWriter(file2), BUFFER);
        writer.write(header.line);
        writer.write("\n");
        long count = 0;
        while (result.hasNext()) {
            writer.write(result.next().line);
            writer.write('\n');
            count++;
        }
        writer.close();
        reader0.close();
        System.out.println("sorting " + count + " lines took "  + (System.currentTimeMillis()-time)/1000+" seconds");
    }

    private static Iterator<FileIterator.Line> wrapStatistics(final FileIterator reader0) {
        return new IteratorWrapper<FileIterator.Line,FileIterator.Line>(reader0) {
                long time = System.currentTimeMillis();
                @Override
                protected FileIterator.Line underlyingObjectToObject(FileIterator.Line line) {
                    if (line.lineNo % 10000 == 0) {
                        System.out.print(".");
                        if (line.lineNo % 1000000 == 0) {
                            long now = System.currentTimeMillis();
                            System.out.println(" "+line.lineNo+ " " +(now - time)+" ms");
                            time = now;
                        }
                    }

                    return line;
                }
            };
    }

}
