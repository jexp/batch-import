package org.neo4j.batchimport.utils;

import java.io.BufferedReader;
import java.io.FileNotFoundException;
import java.io.FileReader;
import java.io.IOException;
import java.util.Iterator;

/**
* @author Michael Hunger @since 04.11.13
*/
class FileIterator implements Iterator<FileIterator.Line> {
    private final BufferedReader reader;
    private final String file;
    Line line;
    long lineNo;

    public FileIterator(String file) throws FileNotFoundException {
        reader = new BufferedReader(new FileReader(file), RelationshipSorter.BUFFER);
        this.file = file;
        line = readLine();
    }

    public void close() throws IOException {
        reader.close();
    }

    private Line readLine() {
        try {
            String line = reader.readLine();
            if (line==null) return null;
            return Line.from(lineNo++, line);
        } catch (IOException e) {
            throw new RuntimeException("Error reading file "+ file,e);
        }
    }


    public boolean hasNext() {
        return line != null;
    }

    public Line next() {
        Line result=line;
        line = readLine();
        return result;
    }

    public void remove() {
    }
    public static class Line {
        String line;
        long lineNo;
        public static Line from(long lineNo, String line) {
            Line result = new Line();
            result.line = line;
            result.lineNo = lineNo;
            return result;
        }
    }
}
