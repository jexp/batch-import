package org.neo4j.batchimport;

import org.junit.Test;
import org.neo4j.consistency.ConsistencyCheckTool;
import org.neo4j.graphdb.GraphDatabaseService;
import org.neo4j.graphdb.Node;
import org.neo4j.graphdb.Transaction;
import org.neo4j.graphdb.factory.GraphDatabaseFactory;
import org.neo4j.io.fs.FileUtils;

import java.io.File;
import java.io.FileWriter;

import static org.junit.Assert.assertTrue;

/**
 * @author Michael Hunger @since 05.11.13
 */
public class ImporterIntegrationTest {

    public static final String DB_DIRECTORY = "target/index-reuse.db";

    @Test
    public void testMain() throws Exception {
        FileUtils.deleteRecursively(new File(DB_DIRECTORY));
        TestDataGenerator.main("1000","10","A,B,C","sorted");
        Importer.main(DB_DIRECTORY,"nodes.csv","rels.csv");
        ConsistencyCheckTool.main(new String[]{DB_DIRECTORY});
    }

    @Test
    public void testImportHashes() throws Exception {
        FileUtils.deleteRecursively(new File(DB_DIRECTORY));
        FileWriter writer = new FileWriter("target/hashes.csv");
        writer.write("a\n000000F8BE951D6DE6480F4AFDFB670C553E47C0\r\n0000021449360C1A398ED9A18800B2B13AA098A4\r\n00000DABDE4C555FC82F7D534835247B94873C2C\r\n00001BE4128DB41729365A41D3AC1D019E5ED8A6\r\n");
        writer.close();
        Importer.main(DB_DIRECTORY,"target/hashes.csv");
        ConsistencyCheckTool.main(new String[]{DB_DIRECTORY});
        GraphDatabaseService db = new GraphDatabaseFactory().newEmbeddedDatabase(new File(DB_DIRECTORY));
        try (Transaction tx = db.beginTx()) {
            for (Node node : db.getAllNodes()) {
                Object value = node.getProperty("a", null);
                System.out.println("value = " + value);
                assertTrue(value != null);
            }
            tx.success();
        }
        db.shutdown();
    }
}
