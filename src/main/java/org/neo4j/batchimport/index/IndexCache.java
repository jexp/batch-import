package org.neo4j.batchimport.index;

/**
 * @author mh
 * @since 20.05.14
 */
public interface IndexCache {
    void add(Object value);
    void doneInsert();
    int get(Object value);
    void set(Object value, long id);
}
