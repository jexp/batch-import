package org.neo4j.batchimport;

import com.lmax.disruptor.RingBuffer;
import com.lmax.disruptor.SingleThreadedClaimStrategy;
import com.lmax.disruptor.YieldingWaitStrategy;
import com.lmax.disruptor.dsl.Disruptor;
import org.apache.log4j.Logger;
import org.neo4j.batchimport.handlers.*;
import org.neo4j.batchimport.structs.NodeStruct;
import org.neo4j.kernel.impl.nioneo.store.NeoStore;
import org.neo4j.unsafe.batchinsert.BatchInserterImpl;
import org.neo4j.unsafe.batchinsert.BatchInserters;

import java.util.Arrays;
import java.util.Map;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

/**
* @author mh
* @since 27.10.12
*/
public class DisruptorBatchInserter {

    private final static Logger log = Logger.getLogger(DisruptorBatchInserter.class);

    private final static int RING_SIZE = 1 <<  18;

    private Disruptor<NodeStruct> incomingEventDisruptor;
    private final String storeDir;
    private BatchInserterImpl inserter;
    private ExecutorService executor;
    private PropertyEncodingHandler[] propertyMappingHandlers;
    private RelationshipIdHandler relationshipIdHandler;
    private NodeWriteRecordHandler nodeWriter;
    private PropertyWriteRecordHandler propertyWriter;
    private RelationshipWriteHandler relationshipWriter;
    private PropertyRecordCreatorHandler propertyRecordCreatorHandler;
    private final Map<String,String> config;
    private final long nodesToCreate;
    private final NodeStructFactory nodeStructFactory;

    public DisruptorBatchInserter(String storeDir, final Map<String, String> config, int nodesToCreate, final NodeStructFactory nodeStructFactory) {
        this.storeDir = storeDir;
        this.config = config;
        this.nodesToCreate = nodesToCreate;
        this.nodeStructFactory = nodeStructFactory;
    }

    void init() {
        inserter = (BatchInserterImpl) BatchInserters.inserter(storeDir, config);
        nodeStructFactory.init(inserter);
        NeoStore neoStore = inserter.getNeoStore();
        neoStore.getNodeStore().setHighId(nodesToCreate + 1);
        executor = Executors.newFixedThreadPool(Runtime.getRuntime().availableProcessors());
        //final ExecutorService executor = Executors.newCachedThreadPool();

        incomingEventDisruptor = new Disruptor<NodeStruct>(nodeStructFactory, executor, new SingleThreadedClaimStrategy(RING_SIZE), new YieldingWaitStrategy());

        createHandlers(neoStore);

        incomingEventDisruptor.
                handleEventsWith(propertyMappingHandlers).
                then(propertyRecordCreatorHandler, relationshipIdHandler).
                then(nodeWriter, relationshipWriter, propertyWriter); //
    }

    private void createHandlers(NeoStore neoStore) {
        propertyMappingHandlers = PropertyEncodingHandler.createHandlers(inserter);

        propertyRecordCreatorHandler = new PropertyRecordCreatorHandler();
        relationshipIdHandler = new RelationshipIdHandler();

        //nodeWriter = new NodeFileWriteHandler(new File(nodeStore.getStorageFileName()));
        nodeWriter = new NodeWriteRecordHandler(neoStore.getNodeStore());
        propertyWriter = new PropertyWriteRecordHandler(neoStore.getPropertyStore());
        relationshipWriter = new RelationshipWriteHandler(new RelationshipRecordWriter(neoStore.getRelationshipStore()));
        //relationshipWriter = new RelationshipWriteHandler(new RelationshipFileWriter(new File(neoStore.getRelationshipStore().getStorageFileName())));
    }

    void run() {
        RingBuffer<NodeStruct> ringBuffer = incomingEventDisruptor.start();
        long time = System.currentTimeMillis();
        for (long nodeId = 0; nodeId < nodesToCreate; nodeId++) {
            long sequence = ringBuffer.next();
            NodeStruct nodeStruct = ringBuffer.get(sequence).init();

            nodeStructFactory.fillStruct(nodeId,nodeStruct);

            if (nodeId % (nodesToCreate / 10) == 0) log.info(nodeId + " " + (System.currentTimeMillis()-time)+" ms.");
            ringBuffer.publish(sequence);
        }
    }
    void shutdown() {
        incomingEventDisruptor.shutdown();
        executor.shutdown();

        nodeWriter.close();
        propertyWriter.close();
        relationshipWriter.close();

        inserter.shutdown();
    }
    void report() {
        log.info("mapped " + Arrays.deepToString(propertyMappingHandlers));

        log.info("relIds " + relationshipIdHandler);

        log.info("wrote nodes " + nodeWriter);
        log.info("wrote rels " + relationshipWriter);
        log.info("wrote props " + propertyWriter);
    }
}
