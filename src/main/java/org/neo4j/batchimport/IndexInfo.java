package org.neo4j.batchimport;

import java.io.File;
import java.util.Map;

/**
* @author mh
* @since 11.06.13
*/
public class IndexInfo {
    public IndexInfo(String[] args, int offset) {
        this.elementType = args[offset];
        this.indexName = args[offset+1];
        this.indexType = args[offset+2];
        this.indexFileName = args[offset+3];
    }

    public IndexInfo(String elementType, String indexName, String indexType, String indexFileName) {
        if (!(elementType.equals("node_index") || elementType.equals("relationship_index"))) throw new IllegalArgumentException("ElementType has to be node_index or relationship_index, but is "+elementType);
        if (!(indexType.equals("exact") || indexType.equals("fulltext"))) throw new IllegalArgumentException("IndexType has to be exact or fulltext, but is "+indexType);
        this.elementType = elementType;
        this.indexName = indexName;
        this.indexType = indexType;
        this.indexFileName = indexFileName;
    }

    public final String elementType, indexName, indexType, indexFileName;

    public static IndexInfo fromConfigEntry(Map.Entry<String, String> entry) {
        if (!entry.getKey().matches("^batch_import\\.(node|relationship)_index\\..+")) return null;
        final String[] keyParts = entry.getKey().split("\\.", 3);
        final String elementType = keyParts[1];
        final String indexName = keyParts[2];
        final String[] valueParts = entry.getValue().split(":");
        final String indexType = valueParts[0];
        final String indexFileName = valueParts.length > 1 ? valueParts[1] : null;
        return new IndexInfo(elementType,indexName,indexType,indexFileName);
    }

    public boolean isNodeIndex() {
        return elementType.equals("node_index");
    }

    public String getConfigKey() {
        return "batch_import."+elementType+"."+indexName;
    }

    public String getConfigValue() {
        if (indexFileName==null) return indexType;
        return indexType+":"+indexFileName;
    }

    public Map<String, String> addToConfig(Map<String, String> config) {
        config.put(getConfigKey(), getConfigValue());
        return config;
    }

    public boolean shouldImportFile() {
        if (indexFileName == null) return false;
        final File file = new File(indexFileName);
        return file.exists() && file.isFile() && file.canRead();
    }
}
