package org.neo4j.batchimport.index;

import org.mapdb.BTreeKeySerializer;
import org.mapdb.Bind;
import org.mapdb.DB;
import org.mapdb.DBMaker;
import org.mapdb.Fun;
import org.neo4j.graphdb.index.IndexHits;
import org.neo4j.index.lucene.unsafe.batchinsert.LuceneBatchInserterIndexProvider;
import org.neo4j.unsafe.batchinsert.BatchInserter;
import org.neo4j.unsafe.batchinsert.BatchInserterIndex;
import org.neo4j.unsafe.batchinsert.BatchInserterIndexProvider;

import java.util.HashMap;
import java.util.Map;
import java.util.NavigableSet;

public class MapDbCachingIndexProvider implements BatchInserterIndexProvider {
    LuceneBatchInserterIndexProvider luceneIndex;
    private DB db;

    public MapDbCachingIndexProvider(BatchInserter inserter) {
        this(new LuceneBatchInserterIndexProvider(inserter));
    }

    public MapDbCachingIndexProvider(LuceneBatchInserterIndexProvider luceneIndex) {
        this.luceneIndex = luceneIndex;
        db = DBMaker.newTempFileDB().
                asyncFlushDelay(1000).
                cacheSize(1024 * 1024).
                closeOnJvmShutdown().
                deleteFilesAfterClose().
                syncOnCommitDisable().
                writeAheadLogDisable().
                make();
    }

    @Override
    public BatchInserterIndex nodeIndex(String indexName, Map<String, String> config) {
        return new CachingBatchInserterIndex(db,indexName,luceneIndex.nodeIndex(indexName,config));
    }

    @Override
    public BatchInserterIndex relationshipIndex(String indexName, Map<String, String> config) {
        return new CachingBatchInserterIndex(db,indexName,luceneIndex.relationshipIndex(indexName, config));
    }

    @Override
    public void shutdown() {
        luceneIndex.shutdown();
        db.close();
    }

    private static class CachingBatchInserterIndex implements BatchInserterIndex {
        Map<String,NavigableSet<Fun.Tuple2<Object, Long>>> caches = new HashMap<String, NavigableSet<Fun.Tuple2<Object, Long>>>();
        private final DB db;
        private final String indexName;
        private final BatchInserterIndex index;

        public CachingBatchInserterIndex(DB db, String indexName, BatchInserterIndex index) {
            this.db = db;
            this.indexName = indexName;
            this.index = index;
        }
        private NavigableSet<Fun.Tuple2<Object,Long>> getSet(String property) {
            NavigableSet<Fun.Tuple2<Object, Long>> set = caches.get(property);
            if (set != null) return set;
            set=db.<Fun.Tuple2<Object, Long>>createTreeSet(indexName+"."+property,32,false, BTreeKeySerializer.TUPLE2,null);
            caches.put(property,set);
            return set;
        }

        @Override
        public void add(long entityId, Map<String, Object> properties) {
            for (Map.Entry<String, Object> entry : properties.entrySet()) {
                getSet(entry.getKey()).add(Fun.t2(entry.getValue(), entityId));
            }
            index.add(entityId,properties);
        }

        @Override
        public void updateOrAdd(long entityId, Map<String, Object> properties) {
            throw new UnsupportedOperationException();
        }

        @Override
        public IndexHits<Long> get(String key, Object value) {
            final Iterable<Long> values = Bind.findSecondaryKeys(getSet(key), value);
            return new LongIterableIndexHits(values);
        }

        @Override
        public IndexHits<Long> query(String key, Object queryOrQueryObject) {
            throw new UnsupportedOperationException();
        }

        @Override
        public IndexHits<Long> query(Object queryOrQueryObject) {
            throw new UnsupportedOperationException();
        }

        @Override
        public void flush() {
            index.flush();
        }

        @Override
        public void setCacheCapacity(String key, int size) {
            throw new UnsupportedOperationException();
        }

    }

}
