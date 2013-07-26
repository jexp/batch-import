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
    private static final int RELS_PER_BUFFER = 1 * 1024 * 1024;

    private volatile boolean stop;

    private Disruptor<NodeStruct> disruptor;
    private BatchInserterImpl inserter;
    private ExecutorService executor;

    private final NodeStructFactory nodeStructFactory;

    // config options
    private static final int REPORT_ON_NTH = 100;
    private final int ringSize;
    private final String storeDir;
    private final Map<String,String> config;
    private final long nodesToCreate;

    private PropertyEncodingHandler[] propertyMappingHandlers;
    private RelationshipIdHandler relationshipIdHandler;
    private NodeWriteRecordHandler nodeWriter;
    private PropertyWriteRecordHandler propertyWriter;
    private RelationshipWriteHandler relationshipWriter;
    private PropertyRecordCreatorHandler propertyRecordCreatorHandler;
    private ForwardRelationshipUpdateHandler forwardRelationshipUpdateHandler;
    private CleanupMemoryHandler cleanupMemoryHandler;

    public DisruptorBatchInserter(String storeDir, final Map<String, String> config, long nodesToCreate, final NodeStructFactory nodeStructFactory) {
        this.storeDir = storeDir;
        final int minBufferBits = (int) (Math.log(nodesToCreate / 1000) / Math.log(2));
        this.ringSize = 1 << Math.min(minBufferBits,18);
        log.info("Ring size " + ringSize+" processors "+Runtime.getRuntime().availableProcessors());
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
        executor = processors >=8 ? Executors.newFixedThreadPool(processors*2) : Executors.newCachedThreadPool();

        disruptor = new Disruptor<NodeStruct>(nodeStructFactory, executor, new SingleThreadedClaimStrategy(ringSize), new YieldingWaitStrategy());
        disruptor.handleExceptionsWith(new BatchInserterExceptionHandler());
        createHandlers(neoStore, nodeStructFactory);

        disruptor.
                handleEventsWith(propertyMappingHandlers).
                then(propertyRecordCreatorHandler, relationshipIdHandler).
                then(forwardRelationshipUpdateHandler, propertyWriter).
                then(relationshipWriter, nodeWriter).
                then(cleanupMemoryHandler);
    }

    private void createHandlers(NeoStore neoStore, NodeStructFactory nodeStructFactory) {
        propertyMappingHandlers = PropertyEncodingHandler.createHandlers(neoStore.getPropertyStore());

        propertyRecordCreatorHandler = new PropertyRecordCreatorHandler();
        relationshipIdHandler = new RelationshipIdHandler();

        //nodeWriter = new NodeFileWriteHandler(new File(nodeStore.getStorageFileName()));
        nodeWriter = new NodeWriteRecordHandler(neoStore.getNodeStore());
        propertyWriter = new PropertyWriteRecordHandler(neoStore.getPropertyStore());
        final RelationshipRecordWriter relationshipRecordWriter = new RelationshipRecordWriter(neoStore.getRelationshipStore());
        relationshipWriter = new RelationshipWriteHandler(relationshipRecordWriter, nodeStructFactory.getTotalNrOfRels());
        forwardRelationshipUpdateHandler = new ForwardRelationshipUpdateHandler(relationshipRecordWriter, nodeStructFactory.getTotalNrOfRels(),RELS_PER_BUFFER);
        cleanupMemoryHandler = new CleanupMemoryHandler();
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

            if (nodesToCreate> REPORT_ON_NTH && nodeId % (nodesToCreate / REPORT_ON_NTH) == 0) {
                log.info(nodeId + " " + (System.currentTimeMillis()-time)+" ms.");
                time = System.currentTimeMillis();
            }
            ringBuffer.publish(sequence);
        }
    }
    void shutdown() {
        disruptor.shutdown();
        executor.shutdown();

        nodeWriter.close();
        propertyWriter.close();
        forwardRelationshipUpdateHandler.close();
        relationshipWriter.close();

        inserter.shutdown();
    }
    void report() {
        log.info("property mapping " + Arrays.deepToString(propertyMappingHandlers));

        log.info("relationship id generation " + relationshipIdHandler);

        log.info("node writing " + nodeWriter);
        log.info("relationship writing " + relationshipWriter);
        log.info("relationship updates " + forwardRelationshipUpdateHandler);
        log.info("property writing " + propertyWriter);
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
