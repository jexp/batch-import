package org.neo4j.batchimport;

import org.neo4j.batchimport.importer.RelType;
import org.neo4j.batchimport.importer.RowData;
import org.neo4j.index.lucene.unsafe.batchinsert.LuceneBatchInserterIndexProvider;
import org.neo4j.kernel.impl.util.FileUtils;
import org.neo4j.unsafe.batchinsert.BatchInserter;
import org.neo4j.unsafe.batchinsert.BatchInserters;
import org.neo4j.unsafe.batchinsert.BatchInserterIndexProvider;
import org.neo4j.unsafe.batchinsert.BatchInserterIndex;

import java.io.*;
import java.util.*;

import static org.neo4j.index.impl.lucene.LuceneIndexImplementation.EXACT_CONFIG;
import static org.neo4j.index.impl.lucene.LuceneIndexImplementation.FULLTEXT_CONFIG;

public class Importer {
    private static Report report;
    private BatchInserter db;
    private BatchInserterIndexProvider lucene;
    
    public Importer(File graphDb) {
        Map<String, String> config = Utils.config();
                
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
        if (args.length < 3) {
            System.err.println("Usage java -jar batchimport.jar data/dir nodes.csv relationships.csv [node_index node-index-name fulltext|exact nodes_index.csv rel_index rel-index-name fulltext|exact rels_index.csv ....]");
        }
        File graphDb = new File(args[0]);
        File nodesFile = new File(args[1]);
        File relationshipsFile = new File(args[2]);

        if (graphDb.exists()) {
            FileUtils.deleteRecursively(graphDb);
        }
        Importer importer = new Importer(graphDb);
        try {
            if (nodesFile.exists()) {
                importer.importNodes(new FileReader(nodesFile));
            } else {
                System.err.println("Nodes file "+nodesFile+" does not exist");
            }

            if (relationshipsFile.exists()) {
                importer.importRelationships(new FileReader(relationshipsFile));
            } else {
                System.err.println("Relationships file "+relationshipsFile+" does not exist");
            }


            for (int i = 3; i < args.length; i = i + 4) {
                String elementType = args[i];
                String indexName = args[i + 1];
                String indexType = args[i + 2];
                String indexFileName = args[i + 3];
                importer.importIndex(elementType, indexName, indexType, indexFileName);
            }
		} finally {
            importer.finish();
        }
    }

    void finish() {
        lucene.shutdown();
        db.shutdown();
        report.finish();
    }

    void importNodes(Reader reader) throws IOException {
        BufferedReader bf = new BufferedReader(reader);
        final RowData data = new RowData(bf.readLine(), "\t", 0);
        String line;
        report.reset();
        while ((line = bf.readLine()) != null) {
            db.createNode(data.updateMap(line));
            report.dots();
        }
        report.finishImport("Nodes");
    }

    void importRelationships(Reader reader) throws IOException {
        BufferedReader bf = new BufferedReader(reader);
        final RowData data = new RowData(bf.readLine(), "\t", 3);
        Object[] rel = new Object[3];
        final RelType relType = new RelType();
        String line;
        report.reset();
        while ((line = bf.readLine()) != null) {
            final Map<String, Object> properties = data.updateMap(line, rel);
            db.createRelationship(id(rel[0]), id(rel[1]), relType.update(rel[2]), properties);
            report.dots();
        }
        report.finishImport("Relationships");
    }

    void importIndex(String indexName, BatchInserterIndex index, Reader reader) throws IOException {

        BufferedReader bf = new BufferedReader(reader);
        
        final RowData data = new RowData(bf.readLine(), "\t", 1);
        Object[] node = new Object[1];
        String line;
        report.reset();
        while ((line = bf.readLine()) != null) {        
            final Map<String, Object> properties = data.updateMap(line, node);
            index.add(id(node[0]), properties);
            report.dots();
        }
                
        report.finishImport("Done inserting into " + indexName + " Index");
    }

    private BatchInserterIndex nodeIndexFor(String indexName, String indexType) {
        return lucene.nodeIndex(indexName, configFor(indexType));
    }

    private BatchInserterIndex relationshipIndexFor(String indexName, String indexType) {
        return lucene.relationshipIndex(indexName, configFor(indexType));
    }

    private Map<String, String> configFor(String indexType) {
        return indexType.equals("fulltext") ? FULLTEXT_CONFIG : EXACT_CONFIG;
    }

    private long id(Object id) {
        return Long.parseLong(id.toString());
    }

    private void importIndex(String elementType, String indexName, String indexType, String indexFileName) throws IOException {
        File indexFile = new File(indexFileName);
        if (!indexFile.exists()) {
            System.err.println("Index file "+indexFile+" does not exist");
            return;
        }
        BatchInserterIndex index = elementType.equals("node_index") ? nodeIndexFor(indexName, indexType) : relationshipIndexFor(indexName, indexType);
        importIndex(indexName, index, new FileReader(indexFile));
    }
}