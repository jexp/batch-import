package org.neo4j.batchimport;

import org.junit.Before;
import org.junit.Test;
import org.mockito.Matchers;
import org.neo4j.batchimport.index.LongIterableIndexHits;
import org.neo4j.batchimport.utils.Config;
import org.neo4j.graphdb.DynamicLabel;
import org.neo4j.graphdb.Label;
import org.neo4j.graphdb.RelationshipType;
import org.neo4j.index.lucene.unsafe.batchinsert.LuceneBatchInserterIndexProvider;
import org.neo4j.unsafe.batchinsert.BatchInserter;
import org.neo4j.unsafe.batchinsert.BatchInserterIndex;
import org.neo4j.unsafe.batchinsert.BatchInserterIndexProvider;

import java.io.File;
import java.io.StringReader;
import java.util.Arrays;
import java.util.Map;

import static java.util.Arrays.*;
import org.junit.Assert;
import org.mockito.ArgumentCaptor;
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
    public void testImportHashes() throws Exception {
        importer.importNodes(new StringReader("a\n000000F8BE951D6DE6480F4AFDFB670C553E47C0\n0000021449360C1A398ED9A18800B2B13AA098A4\n00000DABDE4C555FC82F7D534835247B94873C2C\n00001BE4128DB41729365A41D3AC1D019E5ED8A6\n"));
        importer.finish();
        verify(inserter, times(1)).createNode(eq(map("a", "000000F8BE951D6DE6480F4AFDFB670C553E47C0")));
        verify(inserter, times(1)).createNode(eq(map("a", "0000021449360C1A398ED9A18800B2B13AA098A4")));
        verify(inserter, times(1)).createNode(eq(map("a", "00000DABDE4C555FC82F7D534835247B94873C2C")));
        verify(inserter, times(1)).createNode(eq(map("a", "00001BE4128DB41729365A41D3AC1D019E5ED8A6")));
    }

    @Test
    public void testImportSimpleNodeWithId() throws Exception {
        importer.importNodes(new StringReader("i:id\ta\n123\tfoo"));
        importer.finish();
        verify(inserter, times(1)).createNode(eq(123L),eq(map("a", "foo")));
    }

    @Test
    public void testImportNodeWithNoLabel() throws Exception {
        importer.importNodes(new StringReader("a\t:label\nfoo\t"));
        importer.finish();
        verify(inserter, times(1)).createNode(eq(map("a", "foo")));
    }
    @Test
    public void testImportNodeWithLabel() throws Exception {
        importer.importNodes(new StringReader("a\t:label\nfoo\tbar"));
        importer.finish();
        verify(inserter, times(1)).createNode(eq(map("a", "foo")),eq(DynamicLabel.label("bar")));
    }

    @Test
    public void testImportNodeWithTwoLabels() throws Exception {
        importer.importNodes(new StringReader("a\t:label\nfoo\tbar,bor"));
        importer.finish();
        verify(inserter, times(1)).createNode(eq(map("a", "foo")),eq(DynamicLabel.label("bar")),eq(DynamicLabel.label("bor")));
    }

    @Test
    public void testImportSimpleNodeWithNewlineAtEnd() throws Exception {
        importer.importNodes(new StringReader("a\nfoo\n"));
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

    /*
    @Test
    public void testImportRelWithIndexLookup() throws Exception {
        when(index.get("a","foo")).thenReturn(new LongIterableIndexHits(asList(42L)));
        importer.importRelationships(new StringReader("a:string:index-a\tb\tTYPE\nfoo\t123\tFOOBAR"));
        importer.finish();
        verify(index, times(1)).get(eq("a"), eq("foo"));
        verify(inserter, times(1)).createRelationship(eq(42L), eq(123L), Matchers.any(RelationshipType.class),eq(map()));
    }*/

    /*
    @Test
    public void testImportRelationshipsWithNonIndexedNodes() throws Exception {
        when(index.get("node","a")).thenReturn(new LongIterableIndexHits(asList(1L)));
        when(index.get("node","b")).thenReturn(new LongIterableIndexHits(Arrays.<Long>asList()));
        importer.importRelationships(new StringReader("node:string:index-a\tnode:string:index-a\ttype\na\ta\tTYPE\na\tb\tTYPE\nb\ta\tTYPE"));
        importer.finish();
        verify(inserter, times(1)).createRelationship(eq(1L), eq(1L), argThat(new RelationshipMatcher("TYPE")),eq(map()));
        verify(inserter, never()).createRelationship(eq(1L), eq(-1L), argThat(new RelationshipMatcher("TYPE")),eq(map()));
        verify(inserter, never()).createRelationship(eq(-1L), eq(1L), argThat(new RelationshipMatcher("TYPE")),eq(map()));
    }*/

    @Test
    public void testImportNodeWithIndividualTypes() throws Exception {
        importer.importNodes(new StringReader("a:int\tb:float\tc:float\n10\t10.0\t1E+10"));
        importer.finish();
        verify(inserter, times(1)).createNode(eq(map("a", 10,"b",10.0F,"c",1E+10F)));
    }
    
    @Test
    public void testImportNodeWithArrayTypes() throws Exception {
        importer.importNodes(new StringReader("a:STRING_ARRAY\tb:float\tc:float\n10,11,12\t10.0\t1E+10"));
        importer.finish();
        String[] expectedArray = {"10","11","12"};
        ArgumentCaptor<Map> argument = ArgumentCaptor.forClass(Map.class);
        verify(inserter, times(1)).createNode(argument.capture());
        Map<String, Object> inputMap = argument.getValue();
        Assert.assertArrayEquals((String[])inputMap.get("a"),expectedArray);   
    }
    
    @Test
    public void testImportNodeWithArrayTypesCustomSeparator() throws Exception {
        Config.ARRAYS_SEPARATOR = "%";
        importer.importNodes(new StringReader("a:STRING_ARRAY\tb:float\tc:float\n10%11%12\t10.0\t1E+10"));
        importer.finish();
        String[] expectedArray = {"10","11","12"};
        ArgumentCaptor<Map> argument = ArgumentCaptor.forClass(Map.class);
        verify(inserter, times(1)).createNode(argument.capture());
        Map<String, Object> inputMap = argument.getValue();
        Assert.assertArrayEquals((String[])inputMap.get("a"),expectedArray);   
    }

    /*
    @Test
    public void testImportSimpleRelationship() throws Exception {
        importer.importRelationships(new StringReader("start\tend\ttype\ta\n1\t2\tTYPE\tfoo"));
        importer.finish();
        verify(inserter, times(1)).createRelationship(eq(1L), eq(2L), argThat(new RelationshipMatcher("TYPE")), eq(map("a", "foo")));
    }

    @Test
    public void testImportSimpleRelationshipWithTypeType() throws Exception {
        importer.importRelationships(new StringReader("start\tend\t:label\ta\n1\t2\tTYPE\tfoo"));
        importer.finish();
        verify(inserter, times(1)).createRelationship(eq(1L), eq(2L), argThat(new RelationshipMatcher("TYPE")), eq(map("a", "foo")));
    }

    @Test
    public void testImportSimpleRelationshipWithNewlineOnce() throws Exception {
        importer.importRelationships(new StringReader("start\tend\ttype\ta\n1\t2\tTYPE\tfoo\n"));
        importer.finish();
        verify(inserter, times(1)).createRelationship(eq(1L), eq(2L), argThat(new RelationshipMatcher("TYPE")), eq(map("a", "foo")));
    }

    @Test
    public void testImportRelationshipWithIndividualTypes() throws Exception {
        importer.importRelationships(new StringReader("start\tend\ttype\ta:int\tb:float\tc:float\n1\t2\tTYPE\t10\t10.0\t1E+10"));
        importer.finish();
        verify(inserter, times(1)).createRelationship(eq(1L), eq(2L), argThat(new RelationshipMatcher("TYPE")), eq(map("a", 10, "b", 10.0F, "c", 1E+10F)));
    }*/
}
