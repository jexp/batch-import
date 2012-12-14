package org.neo4j.batchimport;

import org.junit.Test;
import org.neo4j.batchimport.utils.Chunker;

import java.io.*;

import static org.junit.Assert.assertEquals;

/**
 * @author mh
 * @since 13.11.12
 */
public class StreamTokenizerTest {
    
    String file = "FROM\tTO\tTYPE\tNAME\tAGE:INT\n"
                  +"1\t2\tKNOWS\tFOO\t42\n"
                  +"1\t2\tKNOWS\t\t42"
            ;

    @Test
    public void testReadHeader() throws Exception {
        final BufferedReader reader = new BufferedReader(new StringReader(file));
        final String[] header = reader.readLine().split("\t");
        final Chunker chunker = new Chunker(reader, '\t');
        readLine(header, chunker, "FOO", "42");
        readLine(header, chunker, "", "42");
        assertEquals(null,chunker.nextWord());
    }

    private void readLine(String[] header, Chunker st, Object...values) throws IOException {
        long from = Long.parseLong(st.nextWord());
        assertEquals(1,from);
        long to = Long.parseLong(st.nextWord());
        assertEquals(2,to);
        String type = st.nextWord();
        assertEquals("KNOWS", type);

        for (int i = 3; i < header.length; i++) {
            assertEquals(header[i], values[i - 3], st.nextWord());
        }
    }
}
