package org.neo4j.batchimport.collections;

/**
* @author mh
* @since 27.10.12
*/
public interface ReverseRelationshipMap {
     void add(long nodeId, long relId, boolean outgoing);
     CompactLongRecord retrieve(long nodeId);
}
