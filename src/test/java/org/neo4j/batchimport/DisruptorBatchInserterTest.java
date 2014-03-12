package org.neo4j.batchimport;

import org.junit.Test;
import org.neo4j.batchimport.handlers.RelationshipIdHandler;
import org.neo4j.batchimport.structs.NodeStruct;

/**
 * @author mh
 * @since 02.11.12
 */
public class DisruptorBatchInserterTest {

    @Test
    public void testSelfRelationship() throws Exception {
        final NodeStruct struct = new NodeStruct(0);
        struct.addRel(0, true, 0);
        new RelationshipIdHandler().onEvent(struct,0,false);
        //new RelationshipWriteHandler(new RelationshipRecordWriter(neoStore.getRelationshipStore())).onEvent(struct,0,false);
    }
}
