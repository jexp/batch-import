package org.neo4j.batchimport;

import org.junit.Before;
import org.junit.Test;
import org.mockito.Matchers;
import org.neo4j.batchimport.index.LongIterableIndexHits;
import org.neo4j.batchimport.utils.Config;
import org.neo4j.graphdb.RelationshipType;
import org.neo4j.index.lucene.unsafe.batchinsert.LuceneBatchInserterIndexProvider;
import org.neo4j.unsafe.batchinsert.BatchInserter;
import org.neo4j.unsafe.batchinsert.BatchInserterIndex;
import org.neo4j.unsafe.batchinsert.BatchInserterIndexProvider;

import java.io.File;
import java.io.StringReader;
import java.util.Map;

import static java.util.Arrays.*;
import static org.mockito.Matchers.*;
import static org.mockito.Mockito.*;
import static org.neo4j.helpers.collection.MapUtil.map;

public class ImporterTest {

    private BatchInserter inserter;
    private LuceneBatchInserterIndexProvider provider;
    private Importer importer;
    private BatchInserterIndex index;

    @Before
    public void setUp() throws Exception {
        inserter = mock(BatchInserter.class);
        provider = mock(LuceneBatchInserterIndexProvider.class);
        index = mock(BatchInserterIndex.class);
        when(provider.nodeIndex(eq("index-a"),anyMap())).thenReturn(index);

        final Map<String, String> configData = Config.config("batch.properties");
        new IndexInfo("node_index", "index-a", "exact", null).addToConfig(configData);
        importer = new Importer(File.createTempFile("test", "db"), new Config(configData)) {
            @Override
            protected BatchInserter createBatchInserter(File graphDb, Config config) {
                return inserter;
            }

            @Override
            protected BatchInserterIndexProvider createIndexProvider(boolean luceneOnlyIndex) {
                return provider;
            }
        };
    }

    @Test
    public void testImportSimpleNode() throws Exception {
        importer.importNodes(new StringReader("a\nfoo"));
        importer.finish();
        verify(inserter, times(1)).createNode(eq(map("a", "foo")));
    }
    @Test
    public void testImportSimpleNodeWithUmlauts() throws Exception {
        importer.importNodes(new StringReader("ö\näáß"));
        importer.finish();
        verify(inserter, times(1)).createNode(eq(map("ö", "äáß")));
    }
    @Test
    public void testImportNodeWithMultipleProps() throws Exception {
        importer.importNodes(new StringReader("a\tb\nfoo\tbar"));
        importer.finish();
        verify(inserter, times(1)).createNode(eq(map("a", "foo","b","bar")));
    }
    @Test
    public void testImportNodeWithIndex() throws Exception {
        importer.importNodes(new StringReader("a:string:index-a\tb\nfoo\tbar"));
        importer.finish();
        verify(inserter, times(1)).createNode(eq(map("a", "foo", "b", "bar")));
        verify(index, times(1)).add(eq(0L), eq(map("a", "foo")));
    }

    @Test
    public void testImportRelWithIndexLookup() throws Exception {
        when(index.get("a","foo")).thenReturn(new LongIterableIndexHits(asList(42L)));
        importer.importRelationships(new StringReader("a:string:index-a\tb\tTYPE\nfoo\t123\tFOOBAR"));
        importer.finish();
        verify(index, times(1)).get(eq("a"), eq("foo"));
        verify(inserter, times(1)).createRelationship(eq(42L), eq(123L), Matchers.any(RelationshipType.class),eq(map()));
    }

    @Test
    public void testImportNodeWithIndividualTypes() throws Exception {
        importer.importNodes(new StringReader("a:int\tb:float\tc:float\n10\t10.0\t1E+10"));
        importer.finish();
        verify(inserter, times(1)).createNode(eq(map("a", 10,"b",10.0F,"c",1E+10F)));
    }

    @Test
    public void testImportSimpleRelationship() throws Exception {
        importer.importRelationships(new StringReader("start\tend\ttype\ta\n1\t2\tTYPE\tfoo"));
        importer.finish();
        verify(inserter, times(1)).createRelationship(eq(1L), eq(2L), argThat(new RelationshipMatcher("TYPE")), eq(map("a", "foo")));
    }
    @Test
    public void testImportRelationshipWithIndividualTypes() throws Exception {
        importer.importRelationships(new StringReader("start\tend\ttype\ta:int\tb:float\tc:float\n1\t2\tTYPE\t10\t10.0\t1E+10"));
        importer.finish();
        verify(inserter, times(1)).createRelationship(eq(1L), eq(2L), argThat(new RelationshipMatcher("TYPE")), eq(map("a", 10,"b",10.0F,"c",1E+10F)));
    }
}
