package org.neo4j.batchimport.handlers;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.Arrays;

/**
 * @author mh
 * @since 10.11.12
 */
public class RelationshipUpdateCache implements RelationshipUpdater {
    private static final int BUCKETS = 16;
    public static final int RELS_PER_BUFFER =  20 * (1024 ^ 2);
    private static final int RECORD_SIZE = (Short.SIZE + 3 * Integer.SIZE) / 8;
    private static final int CAPACITY = RELS_PER_BUFFER * RECORD_SIZE;

    private final ByteBuffer[] buffers;

    private final RelationshipUpdater relationshipUpdater;
    private final long shard;
    private Stats[] stats;

    static class Stats {
        int idx;
        long added, written;
        long min=Long.MAX_VALUE,max=Long.MIN_VALUE;

        Stats(int idx) {
            this.idx = idx;
        }

        void add(long relId) {
            if (min>relId) min=relId;
            if (max<relId) max=relId;
            added++;
        }
        void written(long count) {
            written+=count;
        }

        @Override
        public String toString() {
            return String.format("buffer %d min %d max %d added %d written %d%n",idx,min==Long.MAX_VALUE?-1:min,max==Long.MIN_VALUE?-1:max,added,written);
        }
    }
    public RelationshipUpdateCache(RelationshipWriter relationshipUpdater, long total) {
        this.relationshipUpdater = relationshipUpdater;
        this.buffers = createBuffers(BUCKETS, CAPACITY);
        this.stats = createStats();
        shard = total / BUCKETS;
    }

    private Stats[] createStats() {
        Stats[] stats = new Stats[BUCKETS];
        for (int i = 0; i < BUCKETS; i++) {
            stats[i]=new Stats(i);
        }
        return stats;
    }

    private ByteBuffer[] createBuffers(final int buckets, final int capacity) {
        ByteBuffer[] buffers = new ByteBuffer[buckets];
        for (int i = 0; i < buffers.length; i++) {
            buffers[i] = ByteBuffer.allocateDirect(capacity);
        }
        return buffers;
    }

    public void update(long relId, boolean outgoing, long prevId, long nextId) throws IOException {
        ByteBuffer buffer = selectBuffer(relId);

        addToBuffer(buffer, relId, outgoing, prevId, nextId);
        flushBuffer(buffer,false);
    }

    private void addToBuffer(ByteBuffer buffer, long relId, boolean outgoing, long prevId, long nextId) {
        int relIdMod = (int)((relId & 0x700000000L) >> 31); //0..2
        int prevIdMod = prevId <= 0 ? 0 : (int)((prevId & 0x700000000L) >> 28); //3..5
        int nextIdMod = nextId <= 0 ? 0 : (int)((nextId & 0x700000000L) >> 25); //6..8
        final int outgoingMod = (outgoing ? 1 : 0) << 9;
        //                     x        x|xx         xxx       xxx
        short header = (short) (outgoingMod | nextIdMod | prevIdMod |relIdMod);
        buffer.putShort(header).putInt((int)relId).putInt((int) prevId).putInt((int) nextId);
    }

    private void updateFromBuffer(ByteBuffer buffer) throws IOException {
        //                      x        x|xx         xxx       xxx
        short header = buffer.getShort();
        long relId =  readIntAsLong(buffer,header    & 0x07);
        long prevId = readIntAsLong(buffer, header>>3 & 0x07);
        long nextId = readIntAsLong(buffer, header>>6 & 0x07);
        boolean outgoing = (header & 0x0200   /*0010.0000*/) != 0;

        relationshipUpdater.update(relId, outgoing, prevId, nextId);
    }

    private ByteBuffer selectBuffer(long relId) {
        int idx= (int) (relId / shard);
        idx = Math.max(0,Math.min(BUCKETS-1,idx));
        stats[idx].add(relId);
        return buffers[idx];
    }

    private void flushBuffer(ByteBuffer buffer, boolean force) throws IOException {
        if (buffer.position()==0) return;
        if (force || buffer.position()==buffer.limit()) {
            buffer.limit(buffer.position());
            buffer.position(0);
            // long time=System.currentTimeMillis();
            while (buffer.position()!=buffer.limit()) updateFromBuffer(buffer);
            stats[idx(buffer)].written(buffer.position()/RECORD_SIZE);
            // System.out.println("Flushed buffer "+idx(buffer)+" in "+(System.currentTimeMillis()-time)+" ms.");
            buffer.clear().limit(CAPACITY);
        }
    }

    private int idx(ByteBuffer buffer) {
        for (int i = 0; i < BUCKETS; i++) {
            if (buffer==buffers[i]) return i;
        }
        return -1;
    }

    private long readIntAsLong(ByteBuffer buffer, int mod) {
        final int result = buffer.getInt();
        if (result==-1 && mod==0) return -1L;
        return ((long) mod << 31) | ((long) result) & 0xFFFFFFFFL;
    }

    public void close() throws IOException {
        for (ByteBuffer buffer : buffers) {
            flushBuffer(buffer, true);
        }
    }

    @Override
    public String toString() {
        return String.format("RelationshipUpdateCache buffer size %d %n stats %s %n", shard, Arrays.toString(stats));
    }
}
