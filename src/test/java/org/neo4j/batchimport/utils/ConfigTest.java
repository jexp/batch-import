package org.neo4j.batchimport.utils;

import org.junit.Before;
import org.junit.Test;

import java.io.File;
import java.io.IOException;
import java.util.Collection;

import static org.assertj.core.api.Assertions.assertThat;
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
    }

    @Before
    public void setUp() throws Exception {

    }

//        final String[] args = "data/dir nodes.csv relationships.csv [node_index node-index-name fulltext|exact nodes_index.csv rel_index rel-index-name fulltext|exact rels_index.csv ....]".split(" ");

    @Test
    public void testExtractDatabaseDir() throws Exception {
        assertCommandLine("data/dir",
                          "data/dir", Properties.BATCH_IMPORT_GRAPH_DB.key());
    }

    @Test
    public void testToFiles() throws Exception {
        final Collection<File> files = Config.toFiles("null,,foo," + nodesFile.getAbsolutePath(), null);
        assertEquals(1,files.size());
        assertEquals(nodesFile.getAbsolutePath(),files.iterator().next().getAbsolutePath());

    }

    @Test
    public void testExtractNodesFiles() throws Exception {
        assertCommandLine("data/dir "+nodesFile.getAbsolutePath(),
                          nodesFile.getAbsolutePath(), Properties.BATCH_IMPORT_NODES_FILES.key());
    }

    @Test
    public void testExtractRelsFiles() throws Exception {
        assertCommandLine("data/dir "+nodesFile.getAbsolutePath()+" "+relsFile.getAbsolutePath(),
                          relsFile.getAbsolutePath(), Properties.BATCH_IMPORT_RELS_FILES.key());
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

    @Test
    public void should_return_empty_string_for_null_path_prefix() {
        assertThat(Config.normalize(null)).isEqualTo("");
    }

    @Test
    public void should_return_string_as_is_if_ending_slash_is_present() {
        assertThat(Config.normalize("test/")).isEqualTo("test" + File.separatorChar);
    }

    @Test
    public void should_return_string_appended_with_slash_as_is_if_no_ending_slash_is_present() {
        assertThat(Config.normalize("foo")).isEqualTo("foo" + File.separatorChar);
    }

    private void assertCommandLine(String arguments, String expected, String optionName) {
        final String configFileName = testConfigFile.getAbsolutePath();
        final String[] args = (configFileName + " " +arguments).split(" ");
        final Config config = Config.convertArgumentsToConfig(args);
        assertEquals(expected, config.get(optionName));
    }

}
