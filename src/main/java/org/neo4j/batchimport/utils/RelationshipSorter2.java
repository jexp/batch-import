package org.neo4j.batchimport.utils;

import org.neo4j.helpers.collection.ArrayIterator;
import org.neo4j.helpers.collection.IteratorWrapper;

import java.io.BufferedWriter;
import java.io.FileWriter;
import java.io.IOException;
import java.util.Arrays;
import java.util.Iterator;

/**
 * @author Michael Hunger @since 04.11.13
 */
public class RelationshipSorter2 {

    public static final char DELIM = '\t';
    public static final int BUFFER = 1024 * 1024;
    public static final FileIterator.RelStartEndComparator COMPARATOR = new FileIterator.RelStartEndComparator();
    public static final int ARRAY_BUFFER = 10_000_000;

    public static void main(String... args) throws IOException {
        System.err.println("Usage mvn exec:java -Dexec.mainClass=org.neo4j.batchimport.utils.RelationshipSorter2 -Dexec.args='rels.csv rels_sorted.csv'");
        final String file = args[0];
        String file2 = args[1];
        long time = System.currentTimeMillis();
        FileIterator reader0 = new FileIterator(file);
        Iterator<FileIterator.Line> reader = wrapStatistics(reader0);
        FileIterator.Line header = reader.next();
        FileIterator.Line[] lines = new FileIterator.Line[ARRAY_BUFFER];
        int read = readArray(reader, lines);
        Arrays.sort(lines, COMPARATOR);
        long count = writeFile(file2, lines, read);
//        Iterator<FileIterator.Line> result = new ArrayIterator<>(lines);
        // sort array
//        long count = writeFile(file2, header, result);
        reader0.close();
        System.out.println("sorting " + count + " lines took "  + (System.currentTimeMillis()-time)/1000+" seconds");
    }

    private static long writeFile(String file, FileIterator.Line header, Iterator<FileIterator.Line> lines) throws IOException {
        BufferedWriter writer = new BufferedWriter(new FileWriter(file), BUFFER);
        if (header!=null) {
            writer.write(header.line); writer.write("\n");
        }
        long count = 0;
        while (lines.hasNext()) {
            writer.write(lines.next().line); writer.write('\n');
            count++;
        }
        writer.close();
        return count;
    }

    private static long writeFile(String file, FileIterator.Line[] lines, int count) throws IOException {
        BufferedWriter writer = new BufferedWriter(new FileWriter(file), BUFFER);
        for (int i = 0; i < count; i++) {
            writer.write(lines[i].line); writer.write('\n');
        }
        writer.close();
        return count;
    }

    private static int readArray(Iterator<FileIterator.Line> reader, FileIterator.Line[] lines) {
        int i=0;
        int length = lines.length;
        while (i < length && reader.hasNext()) {
            lines[i++] = reader.next();
        }
        return i;
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
