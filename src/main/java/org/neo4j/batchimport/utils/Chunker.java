package org.neo4j.batchimport.utils;

import java.io.IOException;
import java.io.Reader;

/**
* @author mh
* @since 13.11.12
*/
public class Chunker {
    public static final String EOF = null;
    public static final String EOL = "\n".intern();
    public static final String NO_VALUE = "".intern();
    public static final char EOL_CHAR = '\n';
    public static final char EOF_CHAR = (char)-1;
    public static final int PREV_EOL_CHAR = -2;
    private static final int BUFSIZE = 32*1024;
    private final Reader reader;
    private final char delim;
    private final char[] buffer=new char[BUFSIZE];
    private int lastChar = PREV_EOL_CHAR;
    private int pos=BUFSIZE;

    public Chunker(Reader reader, char delim) {
        this.reader = reader;
        this.delim = delim;
    }

    /**
     * @return the token, null for EOF, empty string for no value read (just delim) or "\n" for EOL
     * @throws IOException
     */
    public String nextWord() throws IOException {
        int count = 0;
        int ch;
        if (lastChar == EOF_CHAR) return EOF;
        if (lastChar == EOL_CHAR) {
            lastChar = PREV_EOL_CHAR;
            return EOL;
        }

        if (pos == BUFSIZE) {
            int available = reader.read(buffer);
            pos = 0;
            if (available == -1) {
                available = 0;
            }
            if (available < BUFSIZE) {
                buffer[available] = EOF_CHAR;
            }
        }
        int start = pos;
        while ((ch = buffer[pos++])!=delim && ch!= EOL_CHAR && ch!= EOF_CHAR) {
            count++;
            if (pos == BUFSIZE) {
                System.arraycopy(buffer, start, buffer, 0, count);
                int available = reader.read(buffer, count, BUFSIZE - count);
                pos = count;
                start = 0;
                if (available == -1) {
                    available = 0;
                }
                if (available < BUFSIZE - count) {
                    buffer[available + count] = EOF_CHAR;
                }
            }
        }
        if (count == 0) {
            if (lastChar==PREV_EOL_CHAR && ch== EOF_CHAR) { lastChar=EOF_CHAR;return EOF; }            
            lastChar=ch;
            if (ch == EOF_CHAR) return NO_VALUE;
            if (ch == EOL_CHAR) return NO_VALUE;
            return NO_VALUE;
        }
        lastChar=ch;
        return String.valueOf(buffer, start, count);
    }

}
