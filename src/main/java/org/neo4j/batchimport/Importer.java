package org.neo4j.batchimport;

import org.neo4j.batchimport.importer.ChunkerLineData;
import org.neo4j.batchimport.importer.CsvLineData;
import org.neo4j.batchimport.importer.RelType;
import org.neo4j.batchimport.importer.Type;
import org.neo4j.batchimport.index.MapDbCachingIndexProvider;
import org.neo4j.batchimport.utils.Config;
import org.neo4j.graphdb.DynamicLabel;
import org.neo4j.graphdb.Label;
import org.neo4j.graphdb.index.IndexManager;
import org.neo4j.index.lucene.unsafe.batchinsert.LuceneBatchInserterIndexProvider;
import org.neo4j.kernel.impl.util.FileUtils;
import org.neo4j.unsafe.batchinsert.BatchInserter;
import org.neo4j.unsafe.batchinsert.BatchInserters;
import org.neo4j.unsafe.batchinsert.BatchInserterIndexProvider;
import org.neo4j.unsafe.batchinsert.BatchInserterIndex;
import org.neo4j.graphdb.index.IndexHits;

import java.io.*;
import java.util.*;
import java.util.zip.GZIPInputStream;

import static org.neo4j.batchimport.Utils.join;
import static org.neo4j.index.impl.lucene.LuceneIndexImplementation.EXACT_CONFIG;
import static org.neo4j.index.impl.lucene.LuceneIndexImplementation.FULLTEXT_CONFIG;

public class Importer {
    private static final Map<String, String> SPATIAL_CONFIG = Collections.singletonMap(IndexManager.PROVIDER,"spatial");
    private static final Label[] NO_LABELS = new Label[0];
    public static final int BATCH = 10 * 1000 * 1000;
    private static Report report;
    private final Config config;
    private BatchInserter db;
    private BatchInserterIndexProvider indexProvider;
    Map<String,BatchInserterIndex> indexes=new HashMap<String, BatchInserterIndex>();
    private Label[] labelsArray = NO_LABELS;

    private String primary_key;
    private String rel_primary_key;
    private boolean luceneOnlyIndex;

    private Map<Long, Long> lineno_nodeid = new HashMap<Long, Long>();

    public Importer(File graphDb, final Config config) {
        this.config = config;
        db = createBatchInserter(graphDb, config);

        primary_key = config.getIndexPrimaryKey(); 
        rel_primary_key = config.getRelationshipIndexPrimaryKey(); 
        luceneOnlyIndex = config.isCachedIndexDisabled();
        indexProvider = createIndexProvider(luceneOnlyIndex);
        Collection<IndexInfo> indexInfos = config.getIndexInfos();
        if (indexInfos!=null) {
            for (IndexInfo indexInfo : indexInfos) {
                BatchInserterIndex index = indexInfo.isNodeIndex() ? nodeIndexFor(indexInfo.indexName, indexInfo.indexType) : relationshipIndexFor(indexInfo.indexName, indexInfo.indexType);
                indexes.put(indexInfo.indexName, index);
                
            }
        }
        report = createReport();
        //System.out.println("primary key:"+config.getIndexPrimaryKey());
    }

    protected StdOutReport createReport() {
        return new StdOutReport(BATCH, 100);
    }

    protected BatchInserterIndexProvider createIndexProvider(boolean luceneOnlyIndex) {
        return luceneOnlyIndex ? new LuceneBatchInserterIndexProvider(db) : new MapDbCachingIndexProvider(db);
    }

    protected BatchInserter createBatchInserter(File graphDb, Config config) {
        return BatchInserters.inserter(graphDb.getAbsolutePath(), config.getConfigData());
    }

    // todo multiple nodes and rels files
    // todo nodes and rels-files in config
    // todo graphdb in config
    public static void main(String... args) throws IOException {
        System.err.println("Usage: Importer data/dir nodes.csv relationships.csv [node_index node-index-name fulltext|exact nodes_index.csv rel_index rel-index-name fulltext|exact rels_index.csv ....]");
        System.err.println("Using: Importer "+join(args," "));
        System.err.println();

        final Config config = Config.convertArgumentsToConfig(args);

        File graphDb = new File(config.getGraphDbDirectory());
        if (graphDb.exists() && !config.keepDatabase()) {
            FileUtils.deleteRecursively(graphDb);
        }

        Importer importer = new Importer(graphDb, config);
        importer.doImport();
    }

    void finish() {
        indexProvider.shutdown();
        db.shutdown();
        report.finish();
    }

    void importNodes(Reader reader) throws IOException {
        final LineData data = createLineData(reader, 0);
        long lineno = 0, unique_nodes = 0;
        long hit_cache = 0, hit_index = 0;
        report.reset();
        boolean hasId = data.hasId();
        Map<String, Long> primary_key_cache = new HashMap<String, Long>();
        while (data.processLine(null)) {
            boolean have_node = false;
            if (luceneOnlyIndex && !primary_key.equals("")) {
                for (Map.Entry<String, Map<String, Object>> entry : data.getIndexData().entrySet()) {

                    final BatchInserterIndex index = indexFor(entry.getKey());
                    if (index==null)
                        throw new IllegalStateException("Index "+entry.getKey()+" not configured.");
                    if (primary_key.equals("") == false) {
                        if (entry.getValue().containsKey(primary_key)) {
                            String primary_key_value = (String)(entry.getValue().get(primary_key));
                            if (primary_key_cache.containsKey(primary_key_value)) {
                                hit_cache += 1;
                                lineno_nodeid.put(lineno, primary_key_cache.get(primary_key_value));
                                have_node = true;
                                break;
                            }
                            IndexHits<Long> hits = index.get(primary_key, (entry.getValue()).get(primary_key));
                            if (hits.size() != 0) {
                                //iterator can't recall twice
                                Long nodeid = hits.getSingle();
                                lineno_nodeid.put(lineno, nodeid);
                                primary_key_cache.put(primary_key_value, nodeid);
                                hit_index += 1;
                                have_node = true;
                                hits.close();
                                break;
                            }
                            hits.close();
                        }
                    }
                }
            }
            if (have_node) {
                lineno ++;
                continue;
            }
            String[] labels = data.getTypeLabels();
            long id;
            if (hasId) {
                id = data.getId();
                db.createNode(id, data.getProperties(),labelsFor(labels));
            } else {
                id = db.createNode(data.getProperties(),labelsFor(labels));
            }
            unique_nodes += 1;
            for (Map.Entry<String, Map<String, Object>> entry : data.getIndexData().entrySet()) {
                final BatchInserterIndex index = indexFor(entry.getKey());
                if (index==null)
                    throw new IllegalStateException("Index "+entry.getKey()+" not configured.");
                if (entry.getValue().containsKey(primary_key)) {
                    String primary_key_value = (String)(entry.getValue().get(primary_key));
                    primary_key_cache.put(primary_key_value, id);
                }
                index.add(id, entry.getValue());
            }
            report.dots();
            lineno_nodeid.put(lineno, id);
            if (report.getCount() % BATCH == 0) flushIndexes();
            lineno++;
        }
        System.out.println("lineno_nodeid map:" + lineno_nodeid.size() + ",unique nodes num:" + unique_nodes + ",hit_cache times:" + hit_cache + ", hit_index:" + hit_index);
        flushIndexes();
        report.finishImport("Nodes");
    }

    private Label[] labelsFor(String[] labels) {
        if (labels == null || labels.length == 0) return NO_LABELS;
        if (labels.length != labelsArray.length) labelsArray = new Label[labels.length];
        for (int i = labels.length - 1; i >= 0; i--) {
            if (labelsArray[i] == null || !labelsArray[i].name().equals(labels[i]))
                labelsArray[i] = DynamicLabel.label(labels[i]);
        }
        return labelsArray;
    }

    private long lookup(String index,String property,Object value) {
        Long id = indexFor(index).get(property, value).getSingle();
        return id==null ? -1 : id;
    }

    private BatchInserterIndex indexFor(String index) {
        return indexes.get(index);
    }

    void importRelationships(Reader reader) throws IOException {
        final int offset = 3;
        final LineData data = createLineData(reader, offset);
        final RelType relType = new RelType();
        long unique_nodes = 0;
        long skipped=0;
        report.reset();
        
        long hit_cache = 0, hit_index = 0;
        Map<String, Long> rel_primary_key_cache = new HashMap<String, Long>();

        while (data.processLine(null)) {
            boolean have_rel = false;
            if (luceneOnlyIndex && !rel_primary_key.equals("")) {
                for (Map.Entry<String, Map<String, Object>> entry : data.getIndexData().entrySet()) {
                    final BatchInserterIndex index = indexFor(entry.getKey());
                    if (index==null)
                        throw new IllegalStateException("Index "+entry.getKey()+" not configured.");
                    if (rel_primary_key.equals("") == false) {
                        if (entry.getValue().containsKey(rel_primary_key)) {
                            String rel_primary_key_value = (String)entry.getValue().get(rel_primary_key);
                            if (rel_primary_key_cache.containsKey(rel_primary_key_value)) {
                                hit_cache += 1;
                                have_rel = true;
                                break;
                            }
                            IndexHits<Long> hits = index.get(rel_primary_key, rel_primary_key_value);
                            if (hits.size() != 0) {
                                //iterator can't recall twice
                                Long relid = hits.getSingle();
                                rel_primary_key_cache.put(rel_primary_key_value, relid);
                                hit_index += 1;
                                have_rel = true;
                                hits.close();
                                break;
                            }
                            hits.close();
                        }
                    }
                }
            }
            if (have_rel) {
                continue;
            }
            final Map<String, Object> properties = data.getProperties();
            final long start = lineno_nodeid.get(id(data, 0));
            final long end = lineno_nodeid.get(id(data, 1));
            if (start==-1 || end==-1) {
                skipped++;
                continue;
            }
            final RelType type = relType.update(data.getRelationshipTypeLabel());
            final long id = db.createRelationship(start, end, type, properties);
            unique_nodes += 1;
            for (Map.Entry<String, Map<String, Object>> entry : data.getIndexData().entrySet()) {
                if (entry.getValue().containsKey(rel_primary_key)) {
                    String rel_primary_key_value = (String)(entry.getValue().get(rel_primary_key));
                    rel_primary_key_cache.put(rel_primary_key_value, id);
                }
                indexFor(entry.getKey()).add(id, entry.getValue());
            }
            report.dots();
        }
        System.out.println("import unique relationships num:" + unique_nodes + ",hit_cache times:" + hit_cache + ", hit_index:" + hit_index);
        String msg = "Relationships";
        if (skipped > 0) msg += " skipped (" + skipped + ")";
        report.finishImport(msg);
    }

    private void flushIndexes() {
        for (BatchInserterIndex index : indexes.values()) {
            index.flush();
        }
    }

    private LineData createLineData(Reader reader, int offset) {
        final boolean useQuotes = config.quotesEnabled();
        if (useQuotes) return new CsvLineData(reader, config.getDelimChar(this),offset);
        return new ChunkerLineData(reader, config.getDelimChar(this), offset);
    }

    private long id(LineData data, int column) {
        final LineData.Header header = data.getHeader()[column];
        final Object value = data.getValue(column);
        if (header.indexName == null || header.type == Type.ID) {
            return id(value);
        }
        return lookup(header.indexName, header.name, value);
    }

    void importIndex(String indexName, BatchInserterIndex index, Reader reader) throws IOException {
        final LineData data = createLineData(reader, 1);
        report.reset();
        while (data.processLine(null)) {
            final Map<String, Object> properties = data.getProperties();
            index.add(id(data.getValue(0)), properties);
            report.dots();
        }
                
        report.finishImport("Done inserting into " + indexName + " Index");
    }

    private BatchInserterIndex nodeIndexFor(String indexName, String indexType) {
        return indexProvider.nodeIndex(indexName, configFor(indexType));
    }

    private BatchInserterIndex relationshipIndexFor(String indexName, String indexType) {
        return indexProvider.relationshipIndex(indexName, configFor(indexType));
    }

    private Map<String, String> configFor(String indexType) {
        if (indexType.equalsIgnoreCase("fulltext")) return FULLTEXT_CONFIG;
        if (indexType.equalsIgnoreCase("spatial")) return SPATIAL_CONFIG;
        return EXACT_CONFIG;
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
        importIndex(indexInfo.indexName, indexes.get(indexInfo.indexName), createFileReader(indexFile));
    }

    private void doImport() throws IOException {
        try {
            for (File file : config.getNodesFiles()) {
                importNodes(createFileReader(file));
                System.out.println("node file " + file);
            }

            for (File file : config.getRelsFiles()) {
                importRelationships(createFileReader(file));
                System.out.println("rel file " + file);
            }

            for (IndexInfo indexInfo : config.getIndexInfos()) {
                if (indexInfo.shouldImportFile()) importIndex(indexInfo);
            }
		} finally {
            finish();
        }
    }

    final static int BUFFERED_READER_BUFFER = 4096*512;

    private Reader createFileReader(File file) {
        try {
            final String fileName = file.getName();
            if (fileName.endsWith(".gz") || fileName.endsWith(".zip")) {
                return new InputStreamReader(new GZIPInputStream(new BufferedInputStream(new FileInputStream(file)),BUFFERED_READER_BUFFER));
            }
            final FileReader fileReader = new FileReader(file);
            return new BufferedReader(fileReader,BUFFERED_READER_BUFFER);
        } catch(Exception e) {
            throw new IllegalArgumentException("Error reading file "+file+" "+e.getMessage(),e);
        }
    }

}
