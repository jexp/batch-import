package org.neo4j.batchimport;

import org.junit.Test;
import org.neo4j.consistency.ConsistencyCheckTool;

/**
 * @author Michael Hunger @since 05.11.13
 */
public class ImporterIntegrationTest {

    public static final String DB_DIRECTORY = "target/index-reuse.db";

    @Test
    public void testMain() throws Exception {
        TestDataGenerator.main("1000","10","A,B,C","sorted");
        Importer.main(DB_DIRECTORY,"nodes.csv","rels.csv");
        ConsistencyCheckTool.main(new String[]{DB_DIRECTORY});
    }
}
