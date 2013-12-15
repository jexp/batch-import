package org.neo4j.batchimport.utils;

import org.mapdb.Serializer;

import java.io.*;
import java.util.Comparator;
import java.util.Iterator;

class FileIterator implements Iterator<FileIterator.Line> {
    public static final char DELIM = '\t';
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

    public static class LineSerializer implements Serializer<Line> {
        @Override
        public void serialize(DataOutput dataOutput, Line line) throws IOException {
            dataOutput.writeLong(line.lineNo);
//            dataOutput.writeLong(line.min);
//            dataOutput.writeLong(line.max);
            dataOutput.writeUTF(line.line);
        }

        @Override
        public Line deserialize(DataInput dataInput, int i) throws IOException {
//            return Line.from(dataInput.readLong(),dataInput.readLong(),dataInput.readLong(),dataInput.readUTF());
            return Line.from(dataInput.readLong(),dataInput.readUTF());
        }
    }
    public static class Line {
        String line;
        long lineNo, min, max;
        public static Line from(long lineNo, long min, long max, String line) {
            Line result = new Line();
            result.lineNo = lineNo;
            result.min = min;
            result.max = max;
            result.line = line;
            return result;
        }
        public static Line from(long lineNo, String line) {
            if (lineNo > 0) {
                int idx = line.indexOf(DELIM);
                long start = Long.parseLong(line.substring(0, idx++));
                long end = Long.parseLong(line.substring(idx, line.indexOf(DELIM, idx)));
                return from(lineNo,Math.min(start,end), Math.max(start, end),line);
            } else {
                return from(lineNo,-1, -1,line);
            }
        }
    }

    public static class RelStartEndComparator implements Comparator<Line> {

        public int compare(Line line1, Line line2) {
            int result = Long.compare(line1.min, line2.min);
            if (result == 0) {
                result = Long.compare(line1.max, line2.max);
                if (result == 0) return Long.compare(line1.lineNo, line2.lineNo);
            }
            return result;
        }
    }
}
