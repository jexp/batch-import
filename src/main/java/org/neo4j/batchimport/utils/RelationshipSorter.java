package org.neo4j.batchimport.utils;

import org.mapdb.Pump;
import org.mapdb.SerializerBase;

import java.io.*;
import java.util.Iterator;

/**
 * @author Michael Hunger @since 04.11.13
 */
public class RelationshipSorter {

    public static final char DELIM = '\t';
    public static final int BUFFER = 1024 * 1024;

    public static void main(String... args) throws IOException {
        System.err.println("Usage mvn exec:java -Dexec.mainClass=org.neo4j.batchimport.utils.RelationshipSorter -Dexec.args='rels.csv rels_sorted.csv'");
        final String file = args[0];
        String file2 = args[1];
        FileIterator reader = new FileIterator(file);
        Iterator<FileIterator.Line> result = Pump.sort(reader, 10000000, new RelStartEndComparator(), new SerializerBase());
        BufferedWriter writer = new BufferedWriter(new FileWriter(file2), BUFFER);
        while (result.hasNext()) {
            writer.write(result.next().line);
            writer.write('\n');
        }
        writer.close();
        reader.close();
    }

}
