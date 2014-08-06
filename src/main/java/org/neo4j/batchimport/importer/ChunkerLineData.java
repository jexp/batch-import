package org.neo4j.batchimport.importer;

import org.neo4j.batchimport.utils.Chunker;

import java.io.IOException;
import java.io.Reader;
import java.util.*;

public class ChunkerLineData extends AbstractLineData {
    private final Chunker chunker;

    public ChunkerLineData(Reader reader, char delim, int offset) {
        super(offset);
        chunker = new Chunker(reader, delim);
        initHeaders(createHeaders(readRawRow()));
        createMapData(lineSize, offset);
    }

    protected String[] readRawRow() {
        String value;
        Collection<String> result=new ArrayList<String>();
        do {
            value = nextWord();
            if (Chunker.NO_VALUE != value && !isEndOfLineOrFile(value)) {
                result.add(value);
            }
        } while (!isEndOfLineOrFile(value));
        return result.toArray(new String[result.size()]);
    }

    private String nextWord() {
        try {
            return chunker.nextWord();
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
    }

    protected boolean readLine() {
        String value = null;
        int i=0;
        do {
            value = nextWord();
            if (isEndOfLineOrFile(value)) break;
            if (i==lineSize) {
                do {
                    value = nextWord();
                } while (!isEndOfLineOrFile(value)); // consume until EOL
                break;
            }
            lineData[i] = Chunker.NO_VALUE == value ? null : convert(i, value);
            i++;
        } while (!isEndOfLineOrFile(value));
        if (i<lineSize) {
            Arrays.fill(lineData,i,lineSize,null);
        }
        return value != Chunker.EOF;
    }

    private boolean isEndOfLineOrFile(String value) {
        return Chunker.EOL == value || Chunker.EOF == value;
    }
}
