package org.neo4j.batchimport;

import org.junit.Before;
import org.junit.Test;
import org.neo4j.batchimport.structs.Relationship;

import static org.junit.Assert.assertEquals;

public class RelTest {

    public static final boolean OUTGOING = true;
    public static final boolean INCOMING = false;
    public static final int NODE_ZERO = 0;
    public static final int TYPE = 11;
    public static final int NODE_ONE = 1;
    private Relationship rel;

    @Before
    public void setUp() throws Exception {
        rel = new Relationship(0);
    }

    @Test
    public void testOutgoingZero() throws Exception {
        rel.init(NODE_ZERO, OUTGOING, TYPE);
        assertEquals(0,rel.other);
        assertEquals(OUTGOING,rel.outgoing());
        assertEquals(NODE_ZERO,rel.other());
    }
    @Test
    public void testIncomingZero() throws Exception {
        rel.init(NODE_ZERO, INCOMING, TYPE);
        assertEquals(INCOMING,rel.outgoing());
        assertEquals(NODE_ZERO,rel.other());
    }
    @Test
    public void testOutgoingOne() throws Exception {
        rel.init(NODE_ONE, OUTGOING, TYPE);
        assertEquals(OUTGOING,rel.outgoing());
        assertEquals(NODE_ONE,rel.other());
    }
    @Test
    public void testIncomingOne() throws Exception {
        rel.init(NODE_ONE, INCOMING, TYPE);
        assertEquals(INCOMING,rel.outgoing());
        assertEquals(NODE_ONE,rel.other());
    }
}
