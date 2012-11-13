package org.neo4j.batchimport.handlers;

import com.lmax.disruptor.EventHandler;
import org.neo4j.batchimport.structs.NodeStruct;
import org.neo4j.kernel.impl.nioneo.store.Record;

import java.io.File;
import java.io.FileOutputStream;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;

/**
* @author mh
* @since 27.10.12
*/
public class NodeWriteFileHandler implements EventHandler<NodeStruct> {
    public static final int CAPACITY = (1024 * 1024);
    FileOutputStream os;
    int eob=0;
    private final FileChannel channel;
    private final ByteBuffer buffer;
    private int limit;
    private long written;

    public NodeWriteFileHandler(File file) throws IOException {
        os = new FileOutputStream(file);
        channel = os.getChannel();
        channel.position(0);
        buffer = ByteBuffer.allocateDirect(CAPACITY);
        limit = ((int)(CAPACITY/9))*9;
        buffer.limit(limit);
    }

    @Override
    public void onEvent(NodeStruct event, long sequence, boolean endOfBatch) throws Exception {
        writeRecord(event);
        if (endOfBatch) {
            flush();
        }
    }

    private void flush() throws IOException {
        flushBuffer(true);
        channel.force(true);
        eob++;
    }

    @Override
    public String toString() {
        return "batches "+eob+" written "+written;
    }

    private void writeRecord(NodeStruct record) throws IOException {
        //printNode(record);
        long nextRel = record.firstRel;
        long nextProp = record.firstPropertyId;

        short relModifier = Record.NO_NEXT_RELATIONSHIP.is(nextRel) ? 0 : (short) ((nextRel & 0x700000000L) >> 31);
        short propModifier = Record.NO_NEXT_PROPERTY.is(nextProp) ? 0 : (short) ((nextProp & 0xF00000000L) >> 28);

        // [    ,   x] in use bit
        // [    ,xxx ] higher bits for rel id
        // [xxxx,    ] higher bits for prop id
        short inUseUnsignedByte = Record.IN_USE.byteValue();
        inUseUnsignedByte = (short) (inUseUnsignedByte | relModifier | propModifier);
        buffer.put((byte)inUseUnsignedByte).putInt((int)nextRel).putInt((int) nextProp);
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

    public void close() throws IOException {
        flush();
        channel.close();
        os.close();
    }
}
