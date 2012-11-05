package org.neo4j.batchimport.handlers;

import org.neo4j.kernel.impl.nioneo.store.Record;
import org.neo4j.kernel.impl.nioneo.store.RelationshipStore;

import java.io.File;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.RandomAccessFile;

public class RelationshipFileUpdater {
    final RandomAccessFile file;

    public RelationshipFileUpdater(File relationshipFile) {
        try {
            file = new RandomAccessFile(relationshipFile,"w");
        } catch (FileNotFoundException e) {
            throw new RuntimeException(e);
        }
    }
    public void update(long relId, boolean firstNode, long prevId, long nextId, int typeModValue) {
        try {
            long offset = relId * RelationshipStore.RECORD_SIZE + Byte.SIZE + 2 * Integer.SIZE;
            long prevRelMod = Record.NO_NEXT_RELATIONSHIP.is(prevId) ? 0 : (nextId & 0x700000000L) >> (firstNode ? 7 : 13);
            long nextRelMod = Record.NO_NEXT_RELATIONSHIP.is(nextId) ? 0 : (nextId & 0x700000000L) >> (firstNode ? 10 : 16);
            if (prevRelMod==0 && nextRelMod==0) offset += Integer.SIZE;
            else {
                file.seek(offset);
                file.write((int) (typeModValue | prevRelMod | nextRelMod));
            }
            if (!firstNode) {
                offset+=2*Integer.SIZE;
            }
            if (file.getFilePointer()!=offset) file.seek(offset); // todo performance
            file.write((int)prevId);
            file.write((int)nextId);
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
}
