package org.neo4j.batchimport.importer;

import org.neo4j.batchimport.LineData;
import org.neo4j.batchimport.structs.PropertyHolder;
import org.neo4j.batchimport.utils.Chunker;

import java.io.IOException;
import java.io.Reader;
import java.util.*;

import static org.neo4j.helpers.collection.MapUtil.map;

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
            if (Chunker.NO_VALUE != value && Chunker.EOL != value && Chunker.EOF != value) {
                result.add(value);
            }
        } while (value!=Chunker.EOF && value!=Chunker.EOL);
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
            if (i==lineSize) break;
            value = nextWord();
            if (Chunker.EOL == value || Chunker.EOF == value) break;
            if (Chunker.NO_VALUE != value) {
                lineData[i] = headers[i].type == Type.STRING ? value : headers[i].type.convert(value);
            } else {
                lineData[i] = null;
            }
            i++;
        } while (value!=Chunker.EOF && value!=Chunker.EOL);
        return value != Chunker.EOF;
    }
}
