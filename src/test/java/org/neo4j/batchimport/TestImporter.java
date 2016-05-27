package org.neo4j.batchimport;

import org.junit.Ignore;
import org.neo4j.graphdb.RelationshipType;
import org.neo4j.helpers.collection.MapUtil;
import org.neo4j.index.lucene.unsafe.batchinsert.LuceneBatchInserterIndexProvider;
import org.neo4j.io.fs.FileUtils;
import org.neo4j.unsafe.batchinsert.BatchInserter;
import org.neo4j.unsafe.batchinsert.BatchInserterIndexProvider;
import org.neo4j.unsafe.batchinsert.BatchInserters;

import java.io.File;
import java.io.IOException;
import java.util.Map;
import java.util.Random;

import static org.neo4j.helpers.collection.MapUtil.map;

@Ignore
public class TestImporter {
    public static final int NUM_TYPES = 10;
    enum RelTypes implements RelationshipType {
        ONE,TWO,THREE,FOUR,FIVE,SIX,SEVEN,EIGHT,NINE,TEN
    }

    private static Report report;
    private BatchInserter db;
    private BatchInserterIndexProvider lucene;
    
    public TestImporter(File graphDb) throws IOException {
        if (!new File("batch.properties").exists()) {
            System.out.println("Need a Configuration File");
            return;
        }
        System.out.println("Using Existing Configuration File");

        Map<String, String> config = MapUtil.load(new File("batch.properties"));

        db = createBatchInserter(graphDb, config);
        lucene = createIndexProvider();
        report = createReport();
    }

    protected StdOutReport createReport() {
        return new StdOutReport(10 * 1000 * 1000, 100);
    }

    protected LuceneBatchInserterIndexProvider createIndexProvider() {
        return new LuceneBatchInserterIndexProvider(db);
    }

    protected BatchInserter createBatchInserter(File graphDb, Map<String, String> config) throws IOException {
        return BatchInserters.inserter(new File(graphDb.getAbsolutePath()), config);
    }

    public static void main(String[] args) throws IOException {
        if (args.length != 3) {
            System.err.println("Usage java -jar batchperformance.jar db-dir #nodes #rels/node");
        }
        File graphDb = new File(args[0]);
        int nodesCount = Integer.parseInt(args[1]); // 40M
        int relsPerNode = Integer.parseInt(args[2]); // 10

        if (graphDb.exists()) {
            FileUtils.deleteRecursively(graphDb);
        }
        // int[] targetNodeIds = createTargetNodeIds(nodesCount);
        int[] targetNodeOffsets = createTargetNodeIds(nodesCount);
        long time=System.currentTimeMillis();
        TestImporter importer = new TestImporter(graphDb);
        try {
            importer.createNodes(nodesCount,map("blocked",Boolean.TRUE,"age",42L));
            importer.createRels(nodesCount, relsPerNode, targetNodeOffsets,map("weight",10F));
        } finally {
            importer.finish();
        }
        System.out.println("Import of "+nodesCount+" nodes took "+(System.currentTimeMillis()-time)+" ms.");
    }

    private static int[] createTargetNodeIds(int nodesCount) {
        int[] targetNodes = new int[nodesCount];
        Random rnd=new Random();
        for (int i = 0; i < nodesCount; i++) {
            targetNodes[i]=Math.abs(rnd.nextInt() % nodesCount);
        }
        return targetNodes;
    }

    private static int[] createTargetNodeOffsets(int relsPerNode) {
        int[] targetNodeOffsets = new int[relsPerNode];
        for (int i = 0; i < relsPerNode; i++) {
            targetNodeOffsets[i]=1 << 2 * i;
        }
        return targetNodeOffsets;
    }

    public void createRels(int nodesCount, int relsPerNode, int[] targetNodeOffsets, Map<String, Object> props) {
        Random rnd = new Random();
        RelTypes[] values = RelTypes.values();

        report.reset();
        for (int node = 0; node < nodesCount; node++) {
            final int rels = relsPerNode; // rnd.nextInt(relsPerNode);

            for (int rel = rels; rel >= 0; rel--) {
                // final long node1 = Math.abs(rnd.nextLong() % nodesCount);
                // final long node2 = Math.abs(rnd.nextLong() % nodesCount);
                // final long node2 = (node + rels +1) % nodesCount;
                long node2 = (node + targetNodeOffsets[rel])  % nodesCount;
                db.createRelationship(node, node2, RelTypes.ONE, props); // values[rel % NUM_TYPES]
                report.dots();
            }
        }
        report.finishImport("Relationships");
    }

    private void createNodes(long nodesCount, Map<String, Object> props) {
        report.reset();
        for (int node = 0; node < nodesCount; node++) {
            db.createNode(props);
            report.dots();
        }
        report.finishImport("Nodes");
    }

    void finish() {
        lucene.shutdown();
        db.shutdown();
        report.finish();
    }

    static class StdOutReport implements Report {
        private final long batch;
        private final long dots;
        private long count;
        private long total = System.currentTimeMillis(), time, batchTime;

        public StdOutReport(long batch, int dots) {
            this.batch = batch;
            this.dots = batch / dots;
        }

        @Override
        public void reset() {
            count = 0;
            batchTime = time = System.currentTimeMillis();
        }

        @Override
        public void finish() {
            System.out.println("\nTotal import time: "+ (System.currentTimeMillis() - total) / 1000 + " seconds ");
        }

        @Override
        public void dots() {
            if ((++count % dots) != 0) return;
            System.out.print(".");
            if ((count % batch) != 0) return;
            long now = System.currentTimeMillis();
            System.out.println(" "+ (now - batchTime) + " ms for "+batch);
            batchTime = now;
        }

        @Override
        public void finishImport(String type) {
            System.out.println("\nImporting " + count + " " + type + " took " + (System.currentTimeMillis() - time) / 1000 + " seconds ");
        }

        @Override
        public long getCount() {
            return count;
        }
    }
}
