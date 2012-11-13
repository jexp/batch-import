package org.neo4j.batchimport.utils;

import java.io.IOException;
import java.io.Reader;

/**
* @author mh
* @since 13.11.12
*/
public class Chunker {
    private final Reader reader;
    private final char delim;
    private final char[] buffer=new char[100];

    public Chunker(Reader reader, char delim) {
        this.reader = reader;
        this.delim = delim;
    }
    public String nextWord() throws IOException {
        int count = 0;
        int ch;
        while ((ch = reader.read())!=delim && ch!='\n' && ch!=-1) {
            buffer[count++]=(char)ch;
        }
        if (count == 0) {
            if (ch == -1) return null;
            return "";
        }
        return String.valueOf(buffer,0, count);
    }
}
