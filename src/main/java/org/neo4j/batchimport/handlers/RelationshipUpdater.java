package org.neo4j.batchimport.handlers;

import java.io.IOException;

/**
 * @author mh
 * @since 11.11.12
 */
public interface RelationshipUpdater {
    void update(long relId, boolean outgoing, long prevId, long nextId) throws IOException;
}
