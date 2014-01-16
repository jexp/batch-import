package org.neo4j.batchimport;

import org.junit.Test;
import org.neo4j.consistency.ConsistencyCheckTool;

/**
 * @author Michael Hunger @since 02.11.13
 */
public class ParallelImporterTest {

    @Test
    public void testImportNodes() throws Exception {
        TestDataGenerator.main("1000","10","A,B,C","sorted");
//        ParallelImporter.main("target/1000.db","nodes.csv","rels.csv","1000","4","10","10","2","A,B,C","A,B,C");
        ParallelImporter.main("target/1000.db","nodes.csv","rels.csv","1000","4","10","10","2","A,B,C");
        ConsistencyCheckTool.main(new String[]{"target/1000.db"});
    }
}
