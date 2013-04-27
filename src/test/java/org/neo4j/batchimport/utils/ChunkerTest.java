package org.neo4j.batchimport.utils;

import org.junit.Test;

import java.io.StringReader;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertSame;

public class ChunkerTest {
    @Test
    public void testEmptyFile() throws Exception {
        Chunker chunker = newChunker("");
        assertEquals(Chunker.EOF, chunker.nextWord());
    }
    @Test
    public void testEmptyField() throws Exception {
        Chunker chunker = newChunker("\t");
        assertEquals(Chunker.NO_VALUE, chunker.nextWord());
        assertEquals(Chunker.NO_VALUE, chunker.nextWord());
        assertEquals(Chunker.EOF, chunker.nextWord());
    }
    
    @Test
    public void testEmptyFieldWithNewline() throws Exception {
        Chunker chunker = newChunker("\t\n");
        assertEquals(Chunker.NO_VALUE, chunker.nextWord());
        assertEquals(Chunker.NO_VALUE, chunker.nextWord());
        assertEquals(Chunker.EOL, chunker.nextWord());
        assertEquals(Chunker.EOF, chunker.nextWord());
    }

    @Test
    public void testEmptyLine() throws Exception {
        Chunker chunker = newChunker("\n");
        assertEquals(Chunker.NO_VALUE, chunker.nextWord());
        assertEquals(Chunker.EOL, chunker.nextWord());
        assertEquals(Chunker.EOF, chunker.nextWord());
    }
    @Test
    public void testLineWithFields() throws Exception {
        Chunker chunker = newChunker("a\tb\n");
        assertEquals("a", chunker.nextWord());
        assertEquals("b", chunker.nextWord());
        assertEquals(Chunker.EOL, chunker.nextWord());
        assertSame(Chunker.EOF, chunker.nextWord());
    }
    @Test
    public void testUtf8() throws Exception {
        Chunker chunker = newChunker("ä\tá\n");
        assertEquals("ä", chunker.nextWord());
        assertEquals("á", chunker.nextWord());
        assertEquals(Chunker.EOL, chunker.nextWord());
        assertSame(Chunker.EOF, chunker.nextWord());
    }

    @Test
    public void testLineWithEmptyField() throws Exception {
        Chunker chunker = newChunker("a\t\tb\n");
        assertEquals("a", chunker.nextWord());
        assertEquals(Chunker.NO_VALUE, chunker.nextWord());
        assertEquals("b", chunker.nextWord());
        assertEquals(Chunker.EOL, chunker.nextWord());
        assertSame(Chunker.EOF, chunker.nextWord());
    }
    @Test
    public void testLineWithOnlyEmptyFields() throws Exception {
        Chunker chunker = newChunker("\t\t\t\n");
        assertEquals(Chunker.NO_VALUE, chunker.nextWord());
        assertEquals(Chunker.NO_VALUE, chunker.nextWord());
        assertEquals(Chunker.NO_VALUE, chunker.nextWord());
        assertEquals(Chunker.NO_VALUE, chunker.nextWord());
        assertEquals(Chunker.EOL, chunker.nextWord());
        assertSame(Chunker.EOF, chunker.nextWord());
    }

    @Test
    public void testLineWithEmptyLines() throws Exception {
        Chunker chunker = newChunker("a\t\n\nb\n");
        assertEquals("a", chunker.nextWord());
        assertEquals(Chunker.NO_VALUE, chunker.nextWord());
        assertEquals(Chunker.EOL, chunker.nextWord());
        assertEquals(Chunker.NO_VALUE, chunker.nextWord());
        assertEquals(Chunker.EOL, chunker.nextWord());
        assertEquals("b", chunker.nextWord());
        assertEquals(Chunker.EOL, chunker.nextWord());
        assertSame(Chunker.EOF, chunker.nextWord());
    }



    private Chunker newChunker(String lines) {
        return new Chunker(new StringReader(lines), '\t');
    }
}
