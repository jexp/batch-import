package org.neo4j.batchimport.utils;

import org.junit.Before;
import org.junit.Test;

import java.io.File;
import java.io.FileWriter;
import java.io.IOException;
import java.io.StringWriter;
import java.util.Collection;

import static org.junit.Assert.assertEquals;

public class ConfigTest {

    private final File testConfigFile;
    private final File nodesFile = createTempFile("nodes", "csv");
    private final File relsFile = createTempFile("rels", "csv");

    private File createTempFile(String prefix, String suffix) {
        final File tempFile;
        try {
            tempFile = File.createTempFile(prefix, "." + suffix, new File("target"));
           tempFile.deleteOnExit();
            return tempFile;
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
    }

    public ConfigTest() throws IOException {
        testConfigFile = createTempFile("test", "properties");
        FileWriter fileWriter = new FileWriter(testConfigFile);
        fileWriter.write(Config.ARRAY_SEPARATOR_CONFIG+"=|");
        fileWriter.close();
    }

    @Before
    public void setUp() throws Exception {

    }

//        final String[] args = "data/dir nodes.csv relationships.csv [node_index node-index-name fulltext|exact nodes_index.csv rel_index rel-index-name fulltext|exact rels_index.csv ....]".split(" ");
    

    
    @Test
    public void testExtractDatabaseDir() throws Exception {
        assertCommandLine("data/dir",
                          "data/dir", Config.BATCH_IMPORT_GRAPH_DB);
    }

    @Test
    public void testToFiles() throws Exception {
        final Collection<File> files = Config.toFiles("null,,foo," + nodesFile.getAbsolutePath());
        assertEquals(1,files.size());
        assertEquals(nodesFile.getAbsolutePath(),files.iterator().next().getAbsolutePath());

    }

    @Test
    public void testExtractNodesFiles() throws Exception {
        assertCommandLine("data/dir "+nodesFile.getAbsolutePath(),
                          nodesFile.getAbsolutePath(), Config.BATCH_IMPORT_NODES_FILES);
    }

    @Test
    public void testExtractRelsFiles() throws Exception {
        assertCommandLine("data/dir "+nodesFile.getAbsolutePath()+" "+relsFile.getAbsolutePath(),
                          relsFile.getAbsolutePath(), Config.BATCH_IMPORT_RELS_FILES);
    }

    @Test
    public void testExtractExactNodeIndexFile() throws Exception {
        assertCommandLine("data/dir nodes.csv rels.csv node_index index-name exact node_index.csv",
                          "exact:node_index.csv", Config.NODE_INDEX("index-name"));
    }
    @Test
    public void testExtractFulltextNodeIndexFile() throws Exception {
        assertCommandLine("data/dir nodes.csv rels.csv node_index index-name fulltext node_index.csv",
                          "fulltext:node_index.csv", Config.NODE_INDEX("index-name"));
    }
    @Test
    public void testExtractExactNodeIndex() throws Exception {
        assertCommandLine("data/dir nodes.csv rels.csv node_index index-name exact",
                          "exact", Config.NODE_INDEX("index-name"));
    }
    @Test
    public void testExtractFulltextNodeIndex() throws Exception {
        assertCommandLine("data/dir nodes.csv rels.csv node_index index-name fulltext",
                          "fulltext", Config.NODE_INDEX("index-name"));
    }
    
    @Test
    public void testCustomArraySeparator() throws Exception {
            assertCommandLine("data/dir nodes.csv rels.csv node_index index-name fulltext",
                          "|", Config.ARRAY_SEPARATOR_CONFIG);
            Config.ARRAYS_SEPARATOR =",";
    }

    @Test
    public void testExtractExactRelsIndexFile() throws Exception {
        assertCommandLine("data/dir nodes.csv rels.csv relationship_index index-name exact rels_index.csv",
                          "exact:rels_index.csv", Config.RELATIONSHIP_INDEX("index-name"));
    }

    @Test
    public void testExtractExactRelsIndex() throws Exception {
        assertCommandLine("data/dir nodes.csv rels.csv relationship_index index-name exact",
                          "exact", Config.RELATIONSHIP_INDEX("index-name"));
    }

    @Test(expected = IllegalArgumentException.class)
    public void testFailsOnNoArguments() throws Exception {
        assertCommandLine("",null,null);
    }

    private void assertCommandLine(String arguments, String expected, String optionName) {
        final String configFileName = testConfigFile.getAbsolutePath();
        final String[] args = (configFileName + " " +arguments).split(" ");
        final Config config = Config.convertArgumentsToConfig(args);
        assertEquals(expected, config.get(optionName));
    }

}