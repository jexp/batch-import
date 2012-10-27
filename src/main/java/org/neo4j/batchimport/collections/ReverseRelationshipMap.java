package org.neo4j.batchimport.collections;

/**
* @author mh
* @since 27.10.12
*/
public interface ReverseRelationshipMap {
     void add(int nodeId, int relId);
     int[] remove(int nodeId);
}
