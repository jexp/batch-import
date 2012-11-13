package org.neo4j.batchimport.handlers;

import org.neo4j.batchimport.structs.NodeStruct;
import org.neo4j.batchimport.structs.Relationship;
import org.neo4j.kernel.impl.nioneo.store.Record;
import org.neo4j.kernel.impl.nioneo.store.RelationshipStore;

import java.io.File;
import java.io.FileOutputStream;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;

/**
* @author mh
* @since 27.10.12
*/
public class RelationshipFileWriter implements RelationshipWriter {
    public static final int CAPACITY = (1024 * 1024);
    FileOutputStream os;
    int eob=0;
    private final FileChannel channel;
    private final ByteBuffer buffer;
    private int limit;
    private long written;
    private ByteBuffer updateBuffer;
    private long updated;

    public RelationshipFileWriter(File file) throws IOException {
        os = new FileOutputStream(file);
        channel = os.getChannel();
        channel.position(0);
        buffer = ByteBuffer.allocateDirect(CAPACITY);
        updateBuffer = ByteBuffer.allocateDirect(8); // 2x prev/next pointer
        limit = ((int)(CAPACITY/ RelationshipStore.RECORD_SIZE))*RelationshipStore.RECORD_SIZE;
        buffer.limit(limit);
    }

    @Override
    public void create(long nodeId, NodeStruct event, Relationship relationship, long prevId, long nextId) throws IOException {
        long from = nodeId;
        long id = relationship.id;

        long firstNode, secondNode, firstNextRel, firstPrevRel, secondNextRel, secondPrevRel;

        if (relationship.outgoing()) {
            firstNode = from;
            secondNode = relationship.other();
            firstPrevRel = prevId;
            firstNextRel = nextId;
            secondPrevRel = Record.NO_PREV_RELATIONSHIP.intValue();
            secondNextRel = Record.NO_NEXT_RELATIONSHIP.intValue();
        } else {
            firstNode = relationship.other();
            secondNode = from;
            firstPrevRel = Record.NO_PREV_RELATIONSHIP.intValue();
            firstNextRel = Record.NO_NEXT_RELATIONSHIP.intValue();
            secondPrevRel = prevId;
            secondNextRel = nextId;
        }

        short firstNodeMod = (short)((firstNode & 0x700000000L) >> 31);
        long secondNodeMod = (secondNode & 0x700000000L) >> 4;
        long firstPrevRelMod = firstPrevRel == Record.NO_NEXT_RELATIONSHIP.intValue() ? 0 : (firstPrevRel & 0x700000000L) >> 7;
        long firstNextRelMod = firstNextRel == Record.NO_NEXT_RELATIONSHIP.intValue() ? 0 : (firstNextRel & 0x700000000L) >> 10;
        long secondPrevRelMod = secondPrevRel == Record.NO_NEXT_RELATIONSHIP.intValue() ? 0 : (secondPrevRel & 0x700000000L) >> 13;
        long secondNextRelMod = secondNextRel == Record.NO_NEXT_RELATIONSHIP.intValue() ? 0 : (secondNextRel & 0x700000000L) >> 16;

        long nextProp = relationship.firstPropertyId;
        long nextPropMod = nextProp == Record.NO_NEXT_PROPERTY.intValue() ? 0 : (nextProp & 0xF00000000L) >> 28;

        // [    ,   x] in use flag
        // [    ,xxx ] first node high order bits
        // [xxxx,    ] next prop high order bits
        short inUseUnsignedByte = (short)(Record.IN_USE.byteValue() | firstNodeMod | nextPropMod);

        // [ xxx,    ][    ,    ][    ,    ][    ,    ] second node high order bits,     0x70000000
        // [    ,xxx ][    ,    ][    ,    ][    ,    ] first prev rel high order bits,  0xE000000
        // [    ,   x][xx  ,    ][    ,    ][    ,    ] first next rel high order bits,  0x1C00000
        // [    ,    ][  xx,x   ][    ,    ][    ,    ] second prev rel high order bits, 0x380000
        // [    ,    ][    , xxx][    ,    ][    ,    ] second next rel high order bits, 0x70000
        // [    ,    ][    ,    ][xxxx,xxxx][xxxx,xxxx] type
        int typeInt = (int)(relationship.type | secondNodeMod | firstPrevRelMod | firstNextRelMod | secondPrevRelMod | secondNextRelMod);

        buffer.put( (byte)inUseUnsignedByte ).putInt( (int) firstNode ).putInt((int) secondNode)
              .putInt(typeInt).putInt( (int) firstPrevRel ).putInt( (int) firstNextRel )
              .putInt((int) secondPrevRel).putInt( (int) secondNextRel ).putInt( (int) nextProp );

        flushBuffer(false);
    }

    private void flushBuffer(boolean force) throws IOException {
        if (buffer.position()==0) return;
        if (force || buffer.position()==buffer.limit()) {
            buffer.limit(buffer.position());
            buffer.position(0);
            written += channel.write(buffer);
            buffer.clear().limit(limit);
        }
    }

    /**
     * only works for prevId & nextId <= MAXINT
     */
    @Override
    public boolean update(long id, boolean outgoing, long prevId, long nextId) throws IOException {
        flushBuffer(true);
        long position = id * RelationshipStore.RECORD_SIZE + 1 + 4 + 4 + 4; // inUse, firstNode, secondNode, relType

        if (!outgoing) {
            position += 4 + 4;
        }
        long oldPos = channel.position();
        if (oldPos != position) {
            channel.position(position);
        }

        updateBuffer.position(0);
        updateBuffer.putInt((int) prevId).putInt( (int) nextId ).position(0);

        updated += channel.write(updateBuffer);
        channel.position(oldPos);
        return true;
    }

    @Override
    public void close() throws IOException {
        flush();
        channel.close();
        os.close();
    }

    @Override
    public void flush() throws IOException {
        flushBuffer(true);
        eob++;
        channel.force(true);
    }

    @Override
    public void start(long maxRelationshipId) {
    }
    @Override
    public String toString() {
        return "RelationshipFileWriter: batches "+eob+" written "+written+" updated "+updated;
    }
}
