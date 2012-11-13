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
// todo, allocate 1G buffers, pre-sort them before flush and write them to individual files, then use sort -merge to merge them?
// or do multiple passes on the file, allocate a 30G rel-mmio and write the first 1G, then the second 1G etc. rels
public class RelationshipUpdateFileWriter {
    private static final int RECORD_SIZE = (Long.SIZE * 3 + Byte.SIZE) / 8;
    public static final int CAPACITY = (1024 * 1024) * RECORD_SIZE;
    FileOutputStream os;
    int eob=0;
    private final FileChannel channel;
    private final ByteBuffer buffer;
    private int limit;
    private long written;

    public RelationshipUpdateFileWriter(File file) throws IOException {
        os = new FileOutputStream(file);
        channel = os.getChannel();
        channel.position(0);
        buffer = ByteBuffer.allocateDirect(CAPACITY);
        limit = CAPACITY;
        buffer.limit(limit);
    }

    void update(long relId, boolean outgoing, long prevId, long nextId) throws IOException {
        buffer.putLong(relId).putLong(prevId).putLong(nextId).put((byte) (outgoing ? 1 : 0));
        flushBuffer(false);
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
