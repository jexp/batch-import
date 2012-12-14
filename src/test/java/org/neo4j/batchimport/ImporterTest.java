package org.neo4j.batchimport;

import org.junit.Before;
import org.junit.Test;
import org.mockito.Mockito;
import org.neo4j.index.lucene.unsafe.batchinsert.LuceneBatchInserterIndexProvider;
import org.neo4j.unsafe.batchinsert.BatchInserter;

import java.io.File;
import java.io.StringReader;
import java.util.Map;

import static org.mockito.Matchers.any;
import static org.mockito.Matchers.argThat;
import static org.mockito.Matchers.eq;
import static org.mockito.Mockito.atLeastOnce;
import static org.mockito.Mockito.verify;
import static org.neo4j.helpers.collection.MapUtil.map;

public class ImporterTest {

    private BatchInserter inserter;
    private LuceneBatchInserterIndexProvider index;
    private Importer importer;

    @Before
    public void setUp() throws Exception {
        inserter = Mockito.mock(BatchInserter.class);
        index = Mockito.mock(LuceneBatchInserterIndexProvider.class);

        importer = new Importer(File.createTempFile("test", "db")) {
            @Override
            protected BatchInserter createBatchInserter(File graphDb, Map<String, String> config) {
                return inserter;
            }

            @Override
            protected LuceneBatchInserterIndexProvider createIndexProvider() {
                return index;
            }
        };
    }

    @Test
    public void testImportSimpleNode() throws Exception {
        importer.importNodes(new StringReader("a\nfoo"));
        importer.finish();
        verify(inserter, atLeastOnce()).createNode(eq(map("a", "foo")));
    }
    @Test
    public void testImportNodeWithMultipleProps() throws Exception {
        importer.importNodes(new StringReader("a\tb\nfoo\tbar"));
        importer.finish();
        verify(inserter, atLeastOnce()).createNode(eq(map("a", "foo","b","bar")));
    }

    @Test
    public void testImportNodeWithIndividualTypes() throws Exception {
        importer.importNodes(new StringReader("a:int\tb:float\tc:float\n10\t10.0\t1E+10"));
        importer.finish();
        verify(inserter, atLeastOnce()).createNode(eq(map("a", 10,"b",10.0F,"c",1E+10F)));
    }

    @Test
    public void testImportSimpleRelationship() throws Exception {
        importer.importRelationships(new StringReader("start\tend\ttype\ta\n1\t2\tTYPE\tfoo"));
        importer.finish();
        verify(inserter, atLeastOnce()).createRelationship(eq(1L), eq(2L), argThat(new RelationshipMatcher("TYPE")), eq(map("a", "foo")));
    }
    @Test
    public void testImportRelationshipWithIndividualTypes() throws Exception {
        importer.importRelationships(new StringReader("start\tend\ttype\ta:int\tb:float\tc:float\n1\t2\tTYPE\t10\t10.0\t1E+10"));
        importer.finish();
        verify(inserter, atLeastOnce()).createRelationship(eq(1L), eq(2L), argThat(new RelationshipMatcher("TYPE")), eq(map("a", 10,"b",10.0F,"c",1E+10F)));
    }
}
