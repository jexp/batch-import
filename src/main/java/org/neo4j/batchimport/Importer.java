package org.neo4j.batchimport;

import org.neo4j.graphdb.RelationshipType;
import org.neo4j.helpers.collection.MapUtil;
import org.neo4j.index.lucene.unsafe.batchinsert.LuceneBatchInserterIndexProvider;
import org.neo4j.kernel.impl.util.FileUtils;
import org.neo4j.unsafe.batchinsert.BatchInserter;
import org.neo4j.unsafe.batchinsert.BatchInserterIndexProvider;
import org.neo4j.unsafe.batchinsert.BatchInserters;

import java.io.File;
import java.io.IOException;
import java.util.Collections;
import java.util.Map;
import java.util.Random;

public class Importer {
    public static final int NUM_TYPES = 10;
    enum RelTypes implements RelationshipType {
        ONE,TWO,THREE,FOUR,FIVE,SIX,SEVEN,EIGHT,NINE,TEN
    }

    private static Report report;
    private BatchInserter db;
    private BatchInserterIndexProvider lucene;
    
    public Importer(File graphDb) throws IOException {
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

    protected BatchInserter createBatchInserter(File graphDb, Map<String, String> config) {
        return BatchInserters.inserter(graphDb.getAbsolutePath(), config);
    }

    public static void main(String[] args) throws IOException {
        if (args.length != 3) {
            System.err.println("Usage java -jar batchperformance.jar db-dir #nodes #rels/node");
        }
        File graphDb = new File(args[0]);
        int nodesCount = Integer.parseInt(args[1]);
        int relsPerNode = Integer.parseInt(args[2]);

        if (graphDb.exists()) {
            FileUtils.deleteRecursively(graphDb);
        }
        int[] targetNodeIds = createTargetNodeIds(nodesCount);
        Importer importer = new Importer(graphDb);
        try {
            importer.createNodes(nodesCount);
            importer.createRels(nodesCount, relsPerNode, targetNodeIds);
        } finally {
            importer.finish();
        }
    }

    private static int[] createTargetNodeIds(int nodesCount) {
        int[] targetNodes = new int[nodesCount];
        Random rnd=new Random();
        for (int i = 0; i < nodesCount; i++) {
            targetNodes[i]=Math.abs(rnd.nextInt() % nodesCount);
        }
        return targetNodes;
    }

    public void createRels(int nodesCount, int relsPerNode, int[] targetNodes) {
        Random rnd = new Random();
        RelTypes[] values = RelTypes.values();

        report.reset();
        for (int node = 0; node < nodesCount; node++) {
            final int rels = rnd.nextInt(relsPerNode);

            for (int rel = rels; rel >= 0; rel--) {
                // final long node1 = Math.abs(rnd.nextLong() % nodesCount);
                // final long node2 = Math.abs(rnd.nextLong() % nodesCount);
                // final long node2 = (node + rels +1) % nodesCount;
                long node2 = targetNodes[node];
                db.createRelationship(node, node2, values[rel % NUM_TYPES], Collections.EMPTY_MAP);
                report.dots();
            }
        }
        report.finishImport("Relationships");
    }

    private void createNodes(long nodesCount) {
        report.reset();
        for (int node = 0; node < nodesCount; node++) {
            db.createNode(Collections.EMPTY_MAP);
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
    }
}