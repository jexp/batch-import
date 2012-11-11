package org.neo4j.batchimport.handlers;

import org.neo4j.kernel.impl.nioneo.store.Record;
import org.neo4j.kernel.impl.nioneo.store.RelationshipStore;

import java.io.File;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.RandomAccessFile;

public class RelationshipFileUpdater {
    private static final int INT_SIZE = Integer.SIZE / 8;
    private static final int BYTE_SIZE = Byte.SIZE / 8;
    final RandomAccessFile file;

    public RelationshipFileUpdater(File relationshipFile) {
        try {
            file = new RandomAccessFile(relationshipFile,"rw");
        } catch (FileNotFoundException e) {
            throw new RuntimeException(e);
        }
    }
    public void update(long relId, boolean firstNode, long prevId, long nextId, int typeModValue) {
        try {
            //System.out.printf("Rel[%d] prev %d next %d%n",relId,prevId,nextId);

            long offset = relId * RelationshipStore.RECORD_SIZE + BYTE_SIZE + 2 * INT_SIZE;
            long prevRelMod = Record.NO_NEXT_RELATIONSHIP.is(prevId) ? 0 : (prevId & 0x700000000L) >> (firstNode ? 7 : 13);
            long nextRelMod = Record.NO_NEXT_RELATIONSHIP.is(nextId) ? 0 : (nextId & 0x700000000L) >> (firstNode ? 10 : 16);
            if (prevRelMod==0 && nextRelMod==0) offset += INT_SIZE;
            else {
                file.seek(offset);
                final int typeInt = (int) (typeModValue | prevRelMod | nextRelMod);
                file.writeByte(typeInt);
            }
            if (!firstNode) {
                offset+=2*INT_SIZE;
            }
            if (file.getFilePointer()!=offset) file.seek(offset); // todo performance
            file.writeInt((int)prevId);
            file.writeInt((int)nextId);
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
    }

    public void close() {
        try {
            file.close();
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
    }

    public void flush() {
    }
}
