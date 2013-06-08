package org.neo4j.batchimport.handlers;

import edu.ucla.sspace.util.primitive.IntSet;
import edu.ucla.sspace.util.primitive.TroveIntSet;
import org.apache.log4j.Logger;

import java.io.IOException;
import java.nio.BufferOverflowException;
import java.nio.ByteBuffer;
import java.util.Arrays;

/**
 * @author mh
 * @since 10.11.12
 */
public class RelationshipUpdateCache implements RelationshipUpdater {
    private final static Logger log = Logger.getLogger(RelationshipUpdateCache.class);

    public static final int BUCKETS = 16;
    public static final int RELS_PER_BUFFER =  20 * (1024 * 1024); // defaults to 20M rels per buffer
    public static final int RECORD_SIZE = (Short.SIZE + 3 * Integer.SIZE) / 8;
    public static final int SLEEP_RECOVER_CREATE_TIME_MS = 2000;

    private final ByteBuffer[] buffers;

    private final RelationshipUpdater relationshipUpdater;
    private final int buckets;
    private final int capacity;
    private final long shard;
    private final int relsPerBuffer;
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
            return String.format("buffer %d min %d max %d added %d written %d %n",idx,min==Long.MAX_VALUE?-1:min,max==Long.MIN_VALUE?-1:max,added,written);
        }
    }
    public RelationshipUpdateCache(RelationshipWriter relationshipUpdater, long total, int buckets, int relsPerBuffer) {
        this.relationshipUpdater = relationshipUpdater;
        this.buckets = buckets;
        this.relsPerBuffer = relsPerBuffer;
        this.capacity = relsPerBuffer * RECORD_SIZE;
        this.buffers = createBuffers(buckets, capacity);
        this.stats = createStats();
        shard = total / buckets;
    }

    public RelationshipUpdateCache(RelationshipWriter relationshipUpdater, long total) {
        this(relationshipUpdater,total,BUCKETS,RELS_PER_BUFFER);
    }

    private Stats[] createStats() {
        Stats[] stats = new Stats[buckets];
        for (int i = 0; i < buckets; i++) {
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

    public boolean update(long relId, boolean outgoing, long prevId, long nextId) throws IOException {
        ByteBuffer buffer = selectBuffer(relId);
        flushBuffer(buffer, false);
        addToBuffer(buffer, relId, outgoing, prevId, nextId);
        return true;
    }

    private void addToBuffer(ByteBuffer buffer, long relId, boolean outgoing, long prevId, long nextId) {
        int relIdMod = (int)((relId & 0x700000000L) >> 31); //0..2
        int prevIdMod = prevId <= 0 ? 0 : (int)((prevId & 0x700000000L) >> 28); //3..5
        int nextIdMod = nextId <= 0 ? 0 : (int)((nextId & 0x700000000L) >> 25); //6..8
        final int outgoingMod = (outgoing ? 1 : 0) << 9;
        //                     x        x|xx         xxx       xxx
        short header = (short) (outgoingMod | nextIdMod | prevIdMod |relIdMod);
        try {
            buffer.putShort(header);
            buffer.putInt((int)relId);
            buffer.putInt((int) prevId);
            buffer.putInt((int) nextId);
        } catch (BufferOverflowException e) {
            throw e;
        }
    }

    private boolean updateFromBuffer(ByteBuffer buffer) throws IOException {
        //                      x        x|xx         xxx       xxx
        short header = buffer.getShort();
        long relId =  readIntAsLong(buffer,header    & 0x07);
        long prevId = readIntAsLong(buffer, header>>3 & 0x07);
        long nextId = readIntAsLong(buffer, header>>6 & 0x07);
        boolean outgoing = (header & 0x0200   /*0010.0000*/) != 0;

        return relationshipUpdater.update(relId, outgoing, prevId, nextId);
    }

    private ByteBuffer selectBuffer(long relId) {
        int idx= (int) (relId / shard);
        idx = Math.max(0,Math.min(buckets-1,idx));
        stats[idx].add(relId);
        return buffers[idx];
    }

    private void flushBuffer(ByteBuffer buffer, boolean force) throws IOException {
        if (buffer.position()==0) return;
        if (force || buffer.position()==buffer.limit()) {
            buffer.limit(buffer.position());
            buffer.position(0);
            IntSet failedPositions = new TroveIntSet(100);
            long time=System.currentTimeMillis();
            while (buffer.position() != buffer.limit()) {
                final int position = buffer.position();
                if (!updateFromBuffer(buffer)) failedPositions.add(position);
            }
            if (log.isInfoEnabled()) log.info(String.format("flushed buffer %d, kept %d took %d ms.",idx(buffer),failedPositions.size(),System.currentTimeMillis()-time));
            stats[idx(buffer)].written(buffer.position()/RECORD_SIZE - failedPositions.size());
            buffer.limit(capacity);
            int initialPos=failedPositions.isEmpty() ? 0 : copyFailedPositions(buffer, failedPositions);
            buffer.position(initialPos);
        }
    }

    private int copyFailedPositions(ByteBuffer buffer, IntSet failedPositions) {
        byte[] tmp=new byte[RECORD_SIZE];
        int writePos=0;
        for (Integer pos : failedPositions) {
            buffer.position(pos);
            buffer.get(tmp);
            buffer.position(writePos);
            buffer.put(tmp);
            writePos=buffer.position();
        }
        try {
            // give the relationship-writer time to write out the relationships
            if (writePos==buffer.limit()) Thread.sleep(SLEEP_RECOVER_CREATE_TIME_MS);
        } catch (InterruptedException e) {
            throw new RuntimeException(e);
        }
        return writePos;
    }

    private int idx(ByteBuffer buffer) {
        for (int i = 0; i < buckets; i++) {
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
