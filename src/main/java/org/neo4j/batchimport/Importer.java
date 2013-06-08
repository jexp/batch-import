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
    Map<String,BatchInserterIndex> indexes=new HashMap<String, BatchInserterIndex>();

    public Importer(File graphDb, Collection<IndexInfo> indexInfos) {
        Map<String, String> config = Utils.config();
                
        db = createBatchInserter(graphDb, config);
        lucene = createIndexProvider();
        if (indexInfos!=null) {
            for (IndexInfo indexInfo : indexInfos) {
                BatchInserterIndex index = indexInfo.elementType.equals("node_index") ? nodeIndexFor(indexInfo.indexName, indexInfo.indexType) : relationshipIndexFor(indexInfo.indexName, indexInfo.indexType);
                indexes.put(indexInfo.indexName, index);
            }
        }

        report = createReport();
    }

    protected StdOutReport createReport() {
        return new StdOutReport(10 * 1000 * 1000, 100);
    }

    protected BatchInserterIndexProvider createIndexProvider() {
        return new LuceneBatchInserterIndexProvider(db);
    }

    protected BatchInserter createBatchInserter(File graphDb, Map<String, String> config) {
        return BatchInserters.inserter(graphDb.getAbsolutePath(), config);
    }

    static class IndexInfo {
        IndexInfo(String[] args, int offset) {
            this.elementType = args[offset];
            this.indexName = args[offset+1];
            this.indexType = args[offset+2];
            this.indexFileName = args[offset+3];
        }

        public final String elementType, indexName, indexType, indexFileName;
    }

    public static void main(String[] args) throws IOException {
        if (args.length < 3) {
            System.err.println("Usage java -jar batchimport.jar data/dir nodes.csv relationships.csv [node_index node-index-name fulltext|exact nodes_index.csv rel_index rel-index-name fulltext|exact rels_index.csv ....]");
        }
        File graphDb = new File(args[0]);
        File nodesFile = new File(args[1]);
        File relationshipsFile = new File(args[2]);
        Collection<IndexInfo> indexes = createIndexInfos(args);

        if (graphDb.exists()) {
            FileUtils.deleteRecursively(graphDb);
        }
        Importer importer = new Importer(graphDb,indexes);
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

            for (IndexInfo indexInfo : indexes) {
                importer.importIndex(indexInfo);
            }
		} finally {
            importer.finish();
        }
    }

    private static Collection<IndexInfo> createIndexInfos(String[] args) {
        Collection<IndexInfo> indexes=new ArrayList<IndexInfo>();
        for (int i = 3; i < args.length; i = i + 4) {
            indexes.add(new IndexInfo(args,i));
        }
        return indexes;
    }

    void finish() {
        lucene.shutdown();
        db.shutdown();
        report.finish();
    }

    void importNodes(Reader reader) throws IOException {
        BufferedReader bf = new BufferedReader(reader);
        final LineData data = new RowData(bf.readLine(), "\t", 0);
        String line;
        report.reset();
        while ((line = bf.readLine()) != null) {
            final LineData lineData = data.processLine(line);
            final long id = db.createNode(lineData.getProperties());
            for (Map.Entry<String, Map<String, Object>> entry : lineData.getIndexData().entrySet()) {
                indexFor(entry.getKey()).add(id, entry.getValue());
            }
            report.dots();
        }
        report.finishImport("Nodes");
    }

    private long lookup(String index,String property,Object value) {
        return indexFor(index).get(property, value).getSingle();
    }

    private BatchInserterIndex indexFor(String index) {
        return indexes.get(index);
    }

    void importRelationships(Reader reader) throws IOException {
        BufferedReader bf = new BufferedReader(reader);
        final LineData data = new RowData(bf.readLine(), "\t", 3);
        final RelType relType = new RelType();
        String line;
        report.reset();

        while ((line = bf.readLine()) != null) {
            final LineData lineData = data.processLine(line);
            final Map<String, Object> properties = data.getProperties();
            final long start = id(data, 0);
            final long end = id(data, 1);
            final RelType type = relType.update(data.getTypeLabels()[0]);
            final long id = db.createRelationship(start, end, type, properties);
            for (Map.Entry<String, Map<String, Object>> entry : lineData.getIndexData().entrySet()) {
                indexFor(entry.getKey()).add(id, entry.getValue());
            }
            report.dots();
        }
        report.finishImport("Relationships");
    }

    private long id(LineData data, int column) {
        final LineData.Header header = data.getHeader()[column];
        final Object value = data.getValue(column);
        if (header.indexName == null) {
            return id(value);
        }
        return lookup(header.indexName, header.name, value);
    }

    void importIndex(String indexName, BatchInserterIndex index, Reader reader) throws IOException {

        BufferedReader bf = new BufferedReader(reader);
        
        final LineData data = new RowData(bf.readLine(), "\t", 1);
        String line;
        report.reset();
        while ((line = bf.readLine()) != null) {
            data.processLine(line);
            final Map<String, Object> properties = data.getProperties();
            index.add(id(data.getValue(0)), properties);
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

    private void importIndex(IndexInfo indexInfo) throws IOException {
        File indexFile = new File(indexInfo.indexFileName);
        if (!indexFile.exists()) {
            System.err.println("Index file "+indexFile+" does not exist");
            return;
        }
        importIndex(indexInfo.indexName, indexes.get(indexInfo.indexName), new FileReader(indexFile));
    }
}