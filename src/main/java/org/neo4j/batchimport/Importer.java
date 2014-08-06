package org.neo4j.batchimport;

import org.neo4j.batchimport.importer.ChunkerLineData;
import org.neo4j.batchimport.importer.CsvLineData;
import org.neo4j.batchimport.importer.RelType;
import org.neo4j.batchimport.importer.Type;
import org.neo4j.batchimport.index.ArrayBasedIndexCache;
import org.neo4j.batchimport.index.IndexCache;
import org.neo4j.batchimport.utils.Config;
import org.neo4j.helpers.collection.CombiningIterable;
import org.neo4j.helpers.collection.IterableWrapper;
import org.neo4j.kernel.DefaultFileSystemAbstraction;
import org.neo4j.kernel.impl.util.FileUtils;
import org.neo4j.unsafe.batchinsert.BatchInserterIndex;
import org.neo4j.unsafe.impl.batchimport.BatchImporter;
import org.neo4j.unsafe.impl.batchimport.Configuration;
import org.neo4j.unsafe.impl.batchimport.ParallellBatchImporter;
import org.neo4j.unsafe.impl.batchimport.cache.IdMappers;
import org.neo4j.unsafe.impl.batchimport.input.InputNode;
import org.neo4j.unsafe.impl.batchimport.input.InputRelationship;
import org.neo4j.unsafe.impl.batchimport.staging.CoarseUnboundedProgressExecutionMonitor;
import org.neo4j.unsafe.impl.batchimport.store.ChannelReusingFileSystemAbstraction;

import java.io.*;
import java.util.*;
import java.util.zip.GZIPInputStream;

import static org.neo4j.batchimport.Utils.join;

public class Importer {
    private static final String[] NO_LABELS = new String[0];
    public static final int BATCH = 10 * 1000 * 1000;
    private static final int REL_OFFSET = 3;
    private static Report report;
    private final Config config;
//    private BatchInserterIndexProvider indexProvider;
    private BatchImporter db;
//    Map<String,IndexCache> indexes=new HashMap<String, IndexCache>();

    public Importer(File graphDb, final Config config) {
        this.config = config;
        db = createBatchInserter(graphDb, config);

        final boolean luceneOnlyIndex = config.isCachedIndexDisabled();
//        indexProvider = createIndexProvider(luceneOnlyIndex);
//        Collection<IndexInfo> indexInfos = config.getIndexInfos();
//        if (indexInfos!=null) {
//            for (IndexInfo indexInfo : indexInfos) {
//                BatchInserterIndex index = indexInfo.isNodeIndex() ? nodeIndexFor(indexInfo.indexName, indexInfo.indexType) : relationshipIndexFor(indexInfo.indexName, indexInfo.indexType);
//                indexes.put(indexInfo.indexName, index);
//            }
//        }

        report = createReport();
    }

    protected StdOutReport createReport() {
        return new StdOutReport(BATCH, 100);
    }

    // name:string:User -> User as label ->

    //rel: name:string:User name:string:User type

//    protected BatchInserterIndexProvider createIndexProvider(boolean luceneOnlyIndex) {
//        return luceneOnlyIndex ? new LuceneBatchInserterIndexProvider(db) : new MapDbCachingIndexProvider(db);
//    }

    protected BatchImporter createBatchInserter(File graphDb, Config config) {
        CoarseUnboundedProgressExecutionMonitor monitor = new CoarseUnboundedProgressExecutionMonitor(10_000);
        org.neo4j.kernel.configuration.Config kernelConfig = new org.neo4j.kernel.configuration.Config();
        Configuration configuration = new Configuration.OverrideFromConfig(kernelConfig);
        return new ParallellBatchImporter(graphDb.getAbsolutePath(), new ChannelReusingFileSystemAbstraction(new DefaultFileSystemAbstraction()), configuration,null, monitor);
    }

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
//        indexProvider.shutdown();
        db.shutdown();
    }

    Iterable<InputNode> inputNodesFromFileIterable(final File file) {
        return new Iterable<InputNode>() {
            @Override
            public Iterator<InputNode> iterator() {
                return inputNodesFromFile(file);
            }
        };
    }
    Iterable<InputRelationship> inputRelsFromFileIterable(final File file, final Map<String,IndexCache> indexes) {
        return new Iterable<InputRelationship>() {
            @Override
            public Iterator<InputRelationship> iterator() {
                return inputRelsFromFile(file,indexes);
            }
        };
    }

    private Iterator<InputNode> inputNodesFromFile(final File file) {
        return new Iterator<InputNode>() {
            final LineData data = createLineData(createFileReader(file), 0);
            boolean hasId = data.hasId();
            boolean hasNext = data.processLine(null);
            long id = -1; // todo initial

            public boolean hasNext() {
                return hasNext;
            }

            public InputNode next() {
                id = hasId ? data.getId() : id+1;
                Object[] propertyData = data.getPropertyData();
                InputNode inputNode = new InputNode(id, Arrays.copyOf(propertyData, propertyData.length), null, labelsFor(data.getTypeLabels()), null);
                hasNext = data.processLine(null);
                return inputNode;
            }

            public void remove() { }
        };
    }
    private Iterator<InputRelationship> inputRelsFromFile(final File file, final Map<String,IndexCache> indexes) {
        return new Iterator<InputRelationship>() {
            final LineData data = createLineData(createFileReader(file), REL_OFFSET);
            boolean hasId = data.hasId();
            boolean hasNext = data.processLine(null);
            long id = -1;
            long skipped = 0;

            public boolean hasNext() {
                return hasNext;
            }

            public InputRelationship next() {
                id = hasId ? data.getId() : id+1;
                Object[] propertyData = data.getPropertyData();

                final long start = id(data, 0,indexes);
                final long end = id(data, 1,indexes);
                if (start==-1 || end==-1) {
                    skipped++; // todo prefetch
                }

                InputRelationship relationship = new InputRelationship(id,
                        Arrays.copyOf(propertyData, propertyData.length), null,
                        start,end,
                        data.getRelationshipTypeLabel(), null);
                hasNext = data.processLine(null);
                return relationship;
            }

            public void remove() { }
        };
    }

    private String[] labelsFor(String[] labels) {
        if (labels == null || labels.length == 0) return NO_LABELS;
        return Arrays.copyOf(labels,labels.length);
    }

//    private long lookup(String index,String property,Object value) {
//        Long id = indexFor(index).get(property, value).getSingle();
//        return id==null ? -1 : id;
//    }

//    private BatchInserterIndex indexFor(String index) {
//        return indexes.get(index);
//    }

    void importRelationships(Reader reader,Map<String,IndexCache> indexes) throws IOException {
        final int offset = 3;
        final LineData data = createLineData(reader, offset);
        final RelType relType = new RelType();
        long skipped=0;
        report.reset();

        while (data.processLine(null)) {
            final Map<String, Object> properties = data.getProperties();
            final long start = id(data, 0,indexes);
            final long end = id(data, 1, indexes);
            if (start==-1 || end==-1) {
                skipped++;
                continue;
            }
            final RelType type = relType.update(data.getRelationshipTypeLabel());
//            final long id = db.createRelationship(start, end, type, properties);
//            for (Map.Entry<String, Map<String, Object>> entry : data.getIndexData().entrySet()) {
//                indexFor(entry.getKey()).add(id, entry.getValue());
//            }
            report.dots();
        }
        String msg = "Relationships";
        if (skipped > 0) msg += " skipped (" + skipped + ")";
        report.finishImport(msg);
    }

//    private void flushIndexes() {
//        for (BatchInserterIndex index : indexes.values()) {
//            index.flush();
//        }
//    }

    private LineData createLineData(Reader reader, int offset) {
        final boolean useQuotes = config.quotesEnabled();
        if (useQuotes) return new CsvLineData(reader, config.getDelimChar(this),offset);
        return new ChunkerLineData(reader, config.getDelimChar(this), offset);
    }

    private long id(LineData data, int column,Map<String,IndexCache> indexes) {
        final LineData.Header header = data.getHeader()[column];
        final Object value = data.getValue(column);
        if (header.indexName == null || header.type == Type.ID) {
            return id(value);
        }
        return indexes.get(indexName(header)).get(value);
    }

    private String indexName(LineData.Header header) {
        return header.indexName+":"+header.name;
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

//    private BatchInserterIndex nodeIndexFor(String indexName, String indexType) {
//        return indexProvider.nodeIndex(indexName, configFor(indexType));
//    }
//
//    private BatchInserterIndex relationshipIndexFor(String indexName, String indexType) {
//        return indexProvider.relationshipIndex(indexName, configFor(indexType));
//    }

//    private Map<String, String> configFor(String indexType) {
//        if (indexType.equalsIgnoreCase("fulltext")) return FULLTEXT_CONFIG;
//        if (indexType.equalsIgnoreCase("spatial")) return SPATIAL_CONFIG;
//        return EXACT_CONFIG;
//    }

    private long id(Object id) {
        return Long.parseLong(id.toString());
    }

//    private void importIndex(IndexInfo indexInfo) throws IOException {
//        File indexFile = new File(indexInfo.indexFileName);
//        if (!indexFile.exists()) {
//            System.err.println("Index file "+indexFile+" does not exist");
//            return;
//        }
//        importIndex(indexInfo.indexName, indexes.get(indexInfo.indexName), createFileReader(indexFile));
//    }

    private void doImport() throws IOException {
        try {
            final Map<String, IndexCache> indexes = preloadIndexes(config.getNodesFiles());
            db.doImport(new CombiningIterable<InputNode>(
               new IterableWrapper<Iterable<InputNode>,File>(config.getNodesFiles()) {
                   @Override
                   protected Iterable<InputNode> underlyingObjectToObject(File file) {
                        return inputNodesFromFileIterable(file);
                   }
               }
            ),
            new CombiningIterable<InputRelationship>(
                 new IterableWrapper<Iterable<InputRelationship>,File>(config.getRelsFiles()) {
                     @Override
                     protected Iterable<InputRelationship> underlyingObjectToObject(File file) {
                         return inputRelsFromFileIterable(file, indexes);
                     }
                 }
            ),
            new IdMappers.ActualIdMapper());
		} finally {
            finish();
        }
    }

    private Map<String, IndexCache> preloadIndexes(Collection<File> nodesFiles) {
        // check against rel-files if you preload
        Map<String,IndexCache> indexMap=new HashMap<>();
        for (File file : nodesFiles) {
            try (Reader reader = createFileReader(file)) {
                long estimatedLineCount = file.length() / 20;
                final LineData data = createLineData(reader, 0);

                LineData.Header[] headers = data.getHeader();
                IndexCache[] indexes = initializeHeaderIndexes(indexMap, (int) estimatedLineCount, headers);
                if (indexes == null) {
                    continue;
                }
                boolean hasId = data.hasId();
                long id = -1; // todo initial
                while (data.processLine(null)) {
                    id = hasId ? data.getId() : id + 1;
                    for (int i = 0; i < indexes.length; i++) {
                        IndexCache index = indexes[i];
                        if (index == null) continue;
                        if (hasId) indexes[i].set(data.getValue(i), id);
                        else indexes[i].add(data.getValue(i));
                    }
                }
            } catch(IOException e) {
                System.err.println("IOError on file " +file+ " "+e.getMessage());
            }
        }
        for (IndexCache cache : indexMap.values()) {
            cache.doneInsert();
        }
        return indexMap;
    }


    private IndexCache[] initializeHeaderIndexes(Map<String, IndexCache> indexMap, int estimatedLineCount, LineData.Header[] headers) {
        IndexCache[] indexes = new IndexCache[headers.length];
        boolean hasIndex = false;
        for (LineData.Header header : headers) {
            // index == label
            String indexName = header.indexName;
            if (indexName == null) continue;
            hasIndex = true;
            indexName = indexName(header);
            IndexCache indexCache = indexMap.get(indexName);
            if (indexCache==null) {
                indexCache = new ArrayBasedIndexCache(indexName, (int) estimatedLineCount);
                indexMap.put(indexName, indexCache);
            }
            indexes[header.column] = indexCache;
        }
        return hasIndex ? null : indexes;
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
