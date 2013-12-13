package org.neo4j.batchimport.utils;

import java.util.Comparator;

/**
 * @author Michael Hunger @since 04.11.13
 */
public class RelStartEndComparator implements Comparator<FileIterator.Line> {
    public int compare(FileIterator.Line line1, FileIterator.Line line2) {
        int idx1 = line1.line.indexOf(RelationshipSorter.DELIM);
        int idx2 = line2.line.indexOf(RelationshipSorter.DELIM);
        long start1 = Long.parseLong(line1.line.substring(0, idx1++));
        long start2 = Long.parseLong(line2.line.substring(0, idx2++));
        long end1 = Long.parseLong(line1.line.substring(idx1, line1.line.indexOf(RelationshipSorter.DELIM, idx1)));
        long end2 = Long.parseLong(line2.line.substring(idx2, line2.line.indexOf(RelationshipSorter.DELIM, idx2)));
        int result = Long.compare(Math.min(start1, end1), Math.min(start2, end2));
        if (result == 0) {
            result = Long.compare(Math.max(start1, end1), Math.max(start2, end2));
            if (result == 0) return Long.compare(line1.lineNo, line2.lineNo);
        }
        return result;
    }
}
