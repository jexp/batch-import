package org.neo4j.batchimport.csv;

import au.com.bytecode.opencsv.CSVReader;
import org.junit.Assert;
import org.junit.Test;

import java.io.IOException;
import java.io.StringReader;

/**
 * @author mh
 * @since 11.06.13
 */
public class OpenCSVTest {

    @Test
    public void testReadLineWithCommaSeparator() throws Exception {
        final StringReader headerWithLine = new StringReader("a,b\n1,42");
        assertReadFile(new CSVReader(headerWithLine), "42");
    }
    @Test
    public void testReadLineWithTabSeparator() throws Exception {
        final StringReader headerWithLine = new StringReader("a\tb\n1\t42");
        assertReadFile(new CSVReader(headerWithLine,'\t'), "42");
    }
    @Test
    public void testReadLineWithTabSeparatorAndDoubleQuotes() throws Exception {
        final StringReader headerWithLine = new StringReader("a\t\"b\"\n1\t\"42\"");
        assertReadFile(new CSVReader(headerWithLine,'\t','"'), "42");
    }

    @Test
    public void testReadLineWithTabSeparatorAndDoubleQuotesWithNewlineInValue() throws Exception {
        final StringReader headerWithLine = new StringReader("a\t\"b\"\n1\t\"4\n2\"");
        assertReadFile(new CSVReader(headerWithLine,'\t','"'), "4\n2");
    }

    private void assertReadFile(CSVReader reader, final String value2) throws IOException {
        final String[] header = reader.readNext();
        Assert.assertArrayEquals(new String[]{"a", "b"}, header);
        final String[] line = reader.readNext();
        Assert.assertArrayEquals(new String[]{"1", value2}, line);
        Assert.assertNull("EOF", reader.readNext());
    }
}
