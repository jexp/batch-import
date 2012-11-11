package org.neo4j.batchimport;

import com.lmax.disruptor.ExceptionHandler;
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

import java.io.File;
import java.util.Arrays;
import java.util.Map;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

/**
* @author mh
* @since 27.10.12
*/

// todo self-relationships
public class DisruptorBatchInserter {

    private final static Logger log = Logger.getLogger(DisruptorBatchInserter.class);

    private final int RING_SIZE;

    private Disruptor<NodeStruct> disruptor;
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
    private volatile boolean stop;
    private RelationshipUpdateHandler relationshipUpdateHandler;

    public DisruptorBatchInserter(String storeDir, final Map<String, String> config, long nodesToCreate, final NodeStructFactory nodeStructFactory) {
        this.storeDir = storeDir;
        final int minBufferBits = 18; // (int) (Math.log(nodesToCreate / 100) / Math.log(2));
        RING_SIZE = 1 << Math.min(minBufferBits,18);
        System.out.println("Ring size "+RING_SIZE);
        this.config = config;
        this.nodesToCreate = nodesToCreate;
        this.nodeStructFactory = nodeStructFactory;
    }

    void init() {
        inserter = (BatchInserterImpl) BatchInserters.inserter(storeDir, config);
        nodeStructFactory.init(inserter);
        NeoStore neoStore = inserter.getNeoStore();
        neoStore.getNodeStore().setHighId(nodesToCreate + 1);
        final int processors = Runtime.getRuntime().availableProcessors();
        executor = processors >=4 ? Executors.newFixedThreadPool(processors) : Executors.newCachedThreadPool();

        disruptor = new Disruptor<NodeStruct>(nodeStructFactory, executor, new SingleThreadedClaimStrategy(RING_SIZE), new YieldingWaitStrategy());
        disruptor.handleExceptionsWith(new BatchInserterExceptionHandler());
        createHandlers(neoStore, nodeStructFactory);

        disruptor.
                handleEventsWith(propertyMappingHandlers).
                then(propertyRecordCreatorHandler, relationshipIdHandler).
                then(relationshipWriter, propertyWriter).
                then(nodeWriter, relationshipUpdateHandler);
    }

    private void createHandlers(NeoStore neoStore, NodeStructFactory nodeStructFactory) {
        propertyMappingHandlers = PropertyEncodingHandler.createHandlers(inserter);

        propertyRecordCreatorHandler = new PropertyRecordCreatorHandler();
        relationshipIdHandler = new RelationshipIdHandler();

        //nodeWriter = new NodeFileWriteHandler(new File(nodeStore.getStorageFileName()));
        nodeWriter = new NodeWriteRecordHandler(neoStore.getNodeStore());
        propertyWriter = new PropertyWriteRecordHandler(neoStore.getPropertyStore());
        relationshipWriter = new RelationshipWriteHandler(new RelationshipRecordWriter(neoStore.getRelationshipStore()));
        relationshipUpdateHandler = new RelationshipUpdateHandler(new File(neoStore.getRelationshipStore().getStorageFileName()));
        
        //relationshipWriter = new RelationshipWriteHandler(new RelationshipFileWriter(new File(neoStore.getRelationshipStore().getStorageFileName())));
    }

    void run() {
        RingBuffer<NodeStruct> ringBuffer = disruptor.start();
        long time = System.currentTimeMillis();
        for (long nodeId = 0; nodeId < nodesToCreate; nodeId++) {
            if (stop) break;
            long sequence = ringBuffer.next();
            NodeStruct nodeStruct = ringBuffer.get(sequence).init();

            nodeStructFactory.fillStruct(nodeId,nodeStruct);
/*
            if (nodeId % (nodesToCreate / 100) == 0) {
                log.info(nodeId + " " + (System.currentTimeMillis()-time)+" ms.");
                time = System.currentTimeMillis();
            }
*/
            ringBuffer.publish(sequence);
        }
    }
    void shutdown() {
        disruptor.shutdown();
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

    private class BatchInserterExceptionHandler implements ExceptionHandler {
        @Override
        public void handleEventException(Throwable throwable, long nodeId, Object record) {
            log.error(String.format("Error for Node %d Record %s",nodeId,record),throwable);
            // TODO alternatively continue and just log the error
            stop = true;
        }

        @Override
        public void handleOnStartException(Throwable throwable) {
            log.error("Error on start ",throwable);
            System.exit(1);
        }

        @Override
        public void handleOnShutdownException(Throwable throwable) {
            log.error("Error on shutdown ",throwable);
            System.exit(1);
        }
    }
}
