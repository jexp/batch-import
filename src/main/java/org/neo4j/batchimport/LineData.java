package org.neo4j.batchimport;

import org.neo4j.batchimport.importer.Type;

import java.util.Map;

public interface LineData {

    class Header {
        public Header(int column, String name, Type type, String indexName) {
            this.column = column;
            this.name = name;
            this.type = type;
            this.indexName = indexName;
        }

        public final int column;
        public final String name;
        public final Type type;
        public final String indexName; // todo index config in config
    }
    boolean processLine(String line);
    Header[] getHeader();
    long getId();
    Map<String,Object> getProperties();
    Map<String,Map<String,Object>> getIndexData();
    String[] getTypeLabels();
    String getRelationshipTypeLabel();
    Object getValue(int column);
    boolean hasId();
}
