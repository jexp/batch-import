package org.neo4j;

import org.junit.Test;
import org.neo4j.graphdb.GraphDatabaseService;
import org.neo4j.graphdb.Transaction;
import org.neo4j.graphdb.factory.GraphDatabaseFactory;
import org.neo4j.kernel.DefaultFileSystemAbstraction;
import org.neo4j.kernel.impl.util.FileUtils;
import org.neo4j.unsafe.impl.batchimport.Configuration;
import org.neo4j.unsafe.impl.batchimport.ParallellBatchImporter;
import org.neo4j.unsafe.impl.batchimport.cache.IdMappers;
import org.neo4j.unsafe.impl.batchimport.input.InputNode;
import org.neo4j.unsafe.impl.batchimport.input.InputRelationship;
import org.neo4j.unsafe.impl.batchimport.staging.CoarseUnboundedProgressExecutionMonitor;
import org.neo4j.unsafe.impl.batchimport.store.ChannelReusingFileSystemAbstraction;

import java.io.File;
import java.io.IOException;
import java.util.Arrays;
import java.util.Iterator;

import static java.util.Arrays.asList;
import static org.junit.Assert.assertEquals;

/**
 * @author mh
 * @since 21.05.14
 */
public class ParallelBatchImporterTest {

    public static final String STORE_DIR = "target/test_par.db";
    public static final IdMappers.ActualIdMapper ID_MAPPER = new IdMappers.ActualIdMapper();

    @Test
    public void testSimpleImport() throws Exception {
        ParallellBatchImporter importer = createImporter();
        importer.doImport(
                asList(new InputNode(0, new Object[]{"name", "Michael"}, null, new String[]{"Person"}, null),
                        new InputNode(1, new Object[]{"name", "Mark"}, null, new String[]{"Person"}, null)),
                asList(new InputRelationship(0, new Object[]{"since", "2012"}, null, 0, 1, "KNOWS", null)), ID_MAPPER
        );
        importer.shutdown();
        GraphDatabaseService db = new GraphDatabaseFactory().newEmbeddedDatabase(STORE_DIR);
        try (Transaction tx = db.beginTx()){
            assertEquals("Michael", db.getNodeById(0).getProperty("name"));
            assertEquals("Mark",db.getNodeById(1).getProperty("name"));
            assertEquals("2012",db.getRelationshipById(0).getProperty("since"));
        }
        db.shutdown();
    }

    private ParallellBatchImporter createImporter() throws IOException {
        FileUtils.deleteRecursively(new File(STORE_DIR));
        CoarseUnboundedProgressExecutionMonitor monitor = new CoarseUnboundedProgressExecutionMonitor(10_000);
        org.neo4j.kernel.configuration.Config kernelConfig = new org.neo4j.kernel.configuration.Config();
        Configuration configuration = new Configuration.OverrideFromConfig(kernelConfig);
        return new ParallellBatchImporter(STORE_DIR, new ChannelReusingFileSystemAbstraction(new DefaultFileSystemAbstraction()), configuration, null, monitor);
    }

    @Test
    public void testImport100kNodes() throws Exception {
        ParallellBatchImporter importer = createImporter();
        final int max = 1_000_000;
        long start = System.currentTimeMillis();
        importer.doImport(
                inputNodes(createSource(max)),
                Arrays.<InputRelationship>asList(), ID_MAPPER);
        importer.shutdown();
        System.out.println("took = " + (System.currentTimeMillis()-start)+" ms");
        GraphDatabaseService db = new GraphDatabaseFactory().newEmbeddedDatabase(STORE_DIR);
        try (Transaction tx = db.beginTx()){
//            for (int i=0;i<100;i++) {
//                System.out.println(db.getNodeById(i).getProperty("name"));
//            }
            assertEquals("John 1", db.getNodeById(0).getProperty("name"));
            assertEquals("John 2",db.getNodeById(1).getProperty("name"));
            assertEquals("2",db.getNodeById(1).getProperty("age"));
        }
        db.shutdown();
    }

    private Iterator<String[]> createSource(final int max) {
        return new Iterator<String[]>() {
                int i=0;
                String[] data = new String[] {"Person","John","13"};
                String[] header = new String[] {"label","name","age"};

                public boolean hasNext() {
                    return i < max;
                }

                public String[] next() {
                    data[1]="John "+i;
                    data[2]=String.valueOf(i);
                    return i++ == 0 ? header : data;
                }

                public void remove() { }
            };
    }

    private Iterable<InputNode> inputNodes(final Iterator<String[]> rows) {
        String[] header = rows.next();
        final Object[] template = new Object[(header.length-1)*2];
        for (int i = 1; i < header.length; i++) {
            template[(i-1)*2]=header[i];
        }
        return new Iterable<InputNode>() {
            private String[] labels;
            private long id = 0;

            public Iterator<InputNode> iterator() {
                return new Iterator<InputNode>() {
                    public boolean hasNext() {
                        return rows.hasNext();
                    }

                    public InputNode next() {
                        String[] row = rows.next();
                        return node(row);
                    }

                    private InputNode node(String[] row) {
                        Object[] properties = Arrays.copyOf(template, template.length);
                        for (int i = 1; i < row.length; i++) {
                            properties[(i-1)*2+1]=row[i];
                        }
//                        if (id < 100) System.out.println(id+" name "+properties[1]);
                        String[] labels = row[0].split(",");
                        return new InputNode(id++, properties, null, labels, null);
                    }
//                    private InputNode node(String[] row) {
//                        if (labels==null ||!row[0].equals(labels[0])) labels = row[0].split(",");
//                        for (int i = 1; i < row.length; i++) {
//                            template[(i-1)*2+1]=row[i];
//                        }
////                        if (id < 100) System.out.println(id+" name "+template[1]);
//                        return new InputNode(id++, template, null, labels, null);
//                    }

                    public void remove() { }
                };
            }
        };
    }
}
