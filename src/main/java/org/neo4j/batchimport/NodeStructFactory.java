package org.neo4j.batchimport;

import com.lmax.disruptor.EventFactory;
import org.neo4j.batchimport.structs.NodeStruct;
import org.neo4j.unsafe.batchinsert.BatchInserterImpl;

/**
 * @author mh
 * @since 27.10.12
 */
public interface NodeStructFactory extends EventFactory<NodeStruct> {
    NodeStruct newInstance();

    void init(BatchInserterImpl inserter);

    void fillStruct(long nodeId, NodeStruct nodeStruct);

    int getRelsPerNode();

    int getMaxRelsPerNode();

    long getTotalNrOfRels();
}
