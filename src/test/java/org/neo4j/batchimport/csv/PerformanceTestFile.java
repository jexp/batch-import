package org.neo4j.batchimport.csv;

import java.io.BufferedWriter;
import java.io.File;
import java.io.FileWriter;
import java.io.IOException;

/**
 * @author mh
 * @since 11.06.13
 */
public class PerformanceTestFile {
    public final static int ROWS = 1 * 1000 * 1000;
    public final static int COLS = 30;
    static final String TEST_CSV = "target/test.csv";

    public static void createTestFile() throws IOException {
        final BufferedWriter writer = new BufferedWriter(new FileWriter(TEST_CSV));
        for (int row = 0; row < ROWS; row++) {
            for (int col = 0; col < COLS; col++) {
                if (col > 0) writer.write('\t');
                writer.write("\"" + String.valueOf(row * col) + "\"");
            }
            writer.write('\n');
        }
        writer.close();
    }

    static void createTestFileIfNeeded() throws IOException {
        if (new File(TEST_CSV).exists()) return;
        createTestFile();
    }
}
