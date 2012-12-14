package org.neo4j.batchimport.handlers;

import org.neo4j.kernel.impl.nioneo.store.Record;

import java.io.IOException;
import java.util.concurrent.ConcurrentHashMap;

/**
 * @author mh
 * @since 11.11.12
 */
public class ForwardRelationshipUpdateManager {
    RelationshipUpdater updater;

    class RelationshipUpdateInfo { // todo replace with long or bytebuffer/array
        volatile long prevId = Record.NO_PREV_RELATIONSHIP.intValue(), relId = -1;
        volatile boolean outgoing;
        private final long firstId;

        public RelationshipUpdateInfo(long firstId) { // todo replace by prevId??
            this.firstId = firstId;
        }

        private void flush(long next, boolean outgoing) throws IOException {
            if (relId != -1) {
                updater.update(relId, this.outgoing, prevId, next);
            }
            this.prevId = relId;
            this.relId = next;
            this.outgoing = outgoing;
        }
    }
    private final ConcurrentHashMap<Long, RelationshipUpdateInfo> storage=new ConcurrentHashMap<Long, RelationshipUpdateInfo>();

    public ForwardRelationshipUpdateManager(RelationshipUpdater updater) {
        this.updater = updater;
    }
    
    public void add(long nodeId, long relId, boolean outgoing) throws IOException {
        RelationshipUpdateInfo info = storage.get(nodeId);
        if (info==null) {
            info = new RelationshipUpdateInfo(relId);
            storage.put(nodeId,info);
        }
        info.flush(relId,outgoing);
    }

    public long done(long nodeId, final long nextRel) throws IOException {
        final RelationshipUpdateInfo info = storage.remove(nodeId);
        if (info!=null) {
            long oldRelId = info.relId;
            info.flush(nextRel, true);
            return oldRelId;
        }
        return Record.NO_PREV_RELATIONSHIP.intValue();
    }

    public Long getFirstRelId(long nodeId) {
        final RelationshipUpdateInfo info = storage.get(nodeId);
        return info!=null ? info.firstId : null;
    }

}
