package org.neo4j.batchimport.handlers;

import org.neo4j.batchimport.structs.NodeStruct;
import org.neo4j.batchimport.structs.Relationship;

import java.io.IOException;

/**
* @author mh
* @since 27.10.12
*/
public interface RelationshipWriter extends RelationshipUpdater {
    void create(long nodeId, NodeStruct event, Relationship relationship, long prevId, long nextId) throws IOException;

    void flush() throws IOException;

    void start(long maxRelationshipId);

    void close() throws IOException;
}
