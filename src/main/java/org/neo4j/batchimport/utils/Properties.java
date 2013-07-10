package org.neo4j.batchimport.utils;

/**
 * Configuration keys
 * @author Florent Biville (@fbiville)
 */
public enum Properties {
    BATCH_IMPORT_PATH_PREFIX("batch_import.file_path"),
    BATCH_IMPORT_RELS_FILES("batch_import.rels_files"),
    BATCH_IMPORT_GRAPH_DB("batch_import.graph_db"),
    BATCH_IMPORT_KEEP_DB("batch_import.keep_db"),
    BATCH_IMPORT_NODES_FILES("batch_import.nodes_files"),
    BATCH_IMPORT_MAPDB_CACHE_DISABLE("batch_import.mapdb_cache.disable"),
    BATCH_IMPORT_CSV_QUOTES("batch_import.csv.quotes"),
    BATCH_IMPORT_CSV_DELIM("batch_import.csv.delim");

    private final String key;

    Properties(String key) {
        this.key = key;
    }

    public String key() {
        return key;
    }
}