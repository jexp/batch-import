package org.neo4j.batchimport.handlers;

import org.junit.Test;
import org.neo4j.batchimport.structs.NodeStruct;
import org.neo4j.batchimport.structs.Relationship;

import java.io.IOException;
import java.util.concurrent.atomic.AtomicInteger;

import static org.junit.Assert.assertEquals;

/**
 * @author mh
 * @since 10.11.12
 */
public class RelationshipUpdateCacheTest {
    @Test
    public void testWriteRelationship() throws Exception {
        assertAddRelationship(1, true, 2, 3);
        assertAddRelationship(1, false, 2, 3);
        assertAddRelationship(1, true, -1, 3);
        assertAddRelationship(1, true, 2, -1);
        assertAddRelationship(1, true, -1, -1);
        assertAddRelationship(1, false, -1, -1);
        assertAddRelationship(100, true, 2, -1);
        assertAddRelationship(500, true, 2, -1);
        assertAddRelationship(1000, true, 2, -1);
        assertAddRelationship(2000, true, 2, -1);
        assertAddRelationship(2000, true, 0x01FFFFFFFFL, -1);
        assertAddRelationship(0x01FFFFFFFFL, true, 2, -1);
        assertAddRelationship(1, true, 2, 0x01FFFFFFFFL);
    }

    @Test
    public void testFillBuffer() throws Exception {
        final AtomicInteger count=new AtomicInteger();
        final RelationshipUpdateCache cache = new RelationshipUpdateCache(new TestRelationshipWriter() {
            @Override
            public boolean update(long _relId, boolean _outgoing, long _prevId, long _nextId) throws IOException {
                assertEquals("relId", 1,_relId);
                assertEquals("outgoing", true,_outgoing);
                assertEquals("prevId", -1,_prevId);
                assertEquals("nextId", 0x01FFFFFFFFL,_nextId);
                count.incrementAndGet();
                return true;
            }
        },1000);
        final int cnt = RelationshipUpdateCache.RELS_PER_BUFFER;
        // almost fill buffer except last element
        for (int i=0;i<cnt-1;i++)
            cache.update(1,true,-1,0x01FFFFFFFFL);

        assertEquals(0,count.get());

        // last in buffer should cause flush
        cache.update(1,true,-1,0x01FFFFFFFFL);
        assertEquals(cnt, count.get());
        // one more shouldn't cause a new flush
        cache.update(1,true,-1,0x01FFFFFFFFL);
        assertEquals(cnt, count.get());
        // close should cause flush
        cache.close();
        assertEquals(cnt + 1, count.get());
    }

    private void assertAddRelationship(final long relId, final boolean outgoing, final long prevId, final long nextId) throws IOException {
        final RelationshipUpdateCache cache = new RelationshipUpdateCache(new TestRelationshipWriter() {
            @Override
            public boolean update(long _relId, boolean _outgoing, long _prevId, long _nextId) throws IOException {
                assertEquals("relId", relId,_relId);
                assertEquals("outgoing", outgoing,_outgoing);
                assertEquals("prevId", prevId,_prevId);
                assertEquals("nextId", nextId,_nextId);
                return true;
            }
        },1000);

        cache.update(relId, outgoing, prevId, nextId);
        cache.close();
    }

    private static abstract class TestRelationshipWriter implements RelationshipWriter {
        @Override
        public void create(long nodeId, NodeStruct event, Relationship relationship, long prevId, long nextId) throws IOException {

        }

        @Override
        public abstract boolean update(long relId, boolean outgoing, long prevId, long nextId) throws IOException;

        @Override
        public void flush() throws IOException {

        }

        @Override
        public void start(long maxRelationshipId) {

        }

        @Override
        public void close() throws IOException {

        }
    }
}
