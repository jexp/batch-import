package org.neo4j.batchimport;

import com.lmax.disruptor.*;
import com.lmax.disruptor.dsl.Disruptor;
import org.apache.log4j.Logger;
import org.neo4j.consistency.ConsistencyCheckTool;
import org.neo4j.kernel.impl.nioneo.store.*;
import org.neo4j.kernel.impl.util.FileUtils;
import org.neo4j.unsafe.batchinsert.BatchInserterImpl;
import org.neo4j.unsafe.batchinsert.BatchInserters;

import java.io.*;
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;
import java.util.Arrays;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.atomic.AtomicLong;

import static java.util.Arrays.asList;
import static org.neo4j.helpers.collection.MapUtil.map;
import static org.neo4j.helpers.collection.MapUtil.stringMap;

// -server -d64 -Xmx4G -XX:+UseParNewGC

// max i/o 180MB/s

// assumptions
// we know all the id's from the input data,
// relationships are pre-sorted outgoing per node

// create property-key index and rel-type key-index upfront
// map rel-types & prop-names to indexes upfront (input data in the publishers)


// for each property create a property block in a list
// aggregate blocks into property records when all props are done
// update property-record-id's within that block, aka-offsets from a base-id + listsize
// property chains
// arrays
// create relationship-chains
// create
// create nodes last


// relationships, similar to properties
// sorted by outgoing from node

public class DisruptorTest {
    private final static Logger log = Logger.getLogger(DisruptorTest.class);
    public static final String STORE_DIR = "target/test-db2";

    // todo move into factory
    private static final int RELS_PER_NODE = 10;
    public static final int REL_PROPERTY_COUNT = 1;
    public static final int NODE_PROPERTY_COUNT = 2;

    // constant values, to avoid boxing every time
    public static final Float WEIGHT = 10F;
    public static final Long VALUE = 42L;

    private final static int RING_SIZE = 1 << 18; // 22
    public static final int ITERATIONS = 1 * 1000 * 1000;

    @SuppressWarnings("unchecked")
    public static void main(String[] args) throws Exception {
        FileUtils.deleteRecursively(new File(STORE_DIR));
        BatchInserterImpl inserter = (BatchInserterImpl) BatchInserters.inserter(STORE_DIR, stringMap("use_memory_mapped_buffers", "false",
                "dump_configuration", "true",
                "cache_type", "none",
                "neostore.nodestore.db.mapped_memory", "50M",
                "neostore.propertystore.db.mapped_memory", "1G",
                "neostore.relationshipstore.db.mapped_memory", "500M"
        ));
        NeoStore neoStore = inserter.getNeoStore();
        NodeStore nodeStore = neoStore.getNodeStore();
        nodeStore.setHighId(ITERATIONS + 1);
        final ExecutorService executor = Executors.newFixedThreadPool(Runtime.getRuntime().availableProcessors()*2);
        //final ExecutorService executor = Executors.newCachedThreadPool();

        int maxPropertyId = inserter.createAllPropertyIndexes(asList("blocked", "age","weight"));
        int maxRelTypeId = inserter.createAllRelTypeIndexes(asList("CONNECTS"));
        int blocked = inserter.getPropertyKeyId("blocked");
        int age = inserter.getPropertyKeyId("age");
        int weight = inserter.getPropertyKeyId("weight");
        int type = inserter.getRelTypeId("CONNECTS");
        System.out.println("rel-type: " + type);
        System.out.println("maxPropertyId = " + maxPropertyId);


        Disruptor<RecordEvent> incomingEventDisruptor =
                new Disruptor<RecordEvent>(new Factory(), executor, new SingleThreadedClaimStrategy(RING_SIZE), new YieldingWaitStrategy());
        IdSettingEventHandler idSettingEventHandler = new IdSettingEventHandler();

        PropertyMappingEventHandler[] propertyMappingHandlers = new PropertyMappingEventHandler[PropertyMappingEventHandler.MASK + 1];
        for (int i = 0; i < propertyMappingHandlers.length; i++) {
            propertyMappingHandlers[i] = new PropertyMappingEventHandler(inserter, i);
        }
        RelationshipRecordCreator[] relationshipRecordCreators = new RelationshipRecordCreator[RelationshipRecordCreator.MASK + 1];
        for (int i = 0; i < relationshipRecordCreators.length; i++) {
            relationshipRecordCreators[i] = new RelationshipRecordCreator(i);
        }
        RelIdSettingEventHandler relIdSettingEventHandler = new RelIdSettingEventHandler();

        //NodeWriter nodeWriter = new NodeWriter(new File(nodeStore.getStorageFileName()));
        NodeRecordWritingEventHandler nodeWriter = new NodeRecordWritingEventHandler(nodeStore);
        PropertyWriter propertyWriter = new PropertyWriter(neoStore.getPropertyStore());
        RecordValidator validator = new RecordValidator();
        RelationshipWriter relationshipWriter = new RelationshipWriter(neoStore.getRelationshipStore(), validator);
        incomingEventDisruptor.
                handleEventsWith(propertyMappingHandlers).
                then(new PropertyRecordEventHandler(),idSettingEventHandler, relIdSettingEventHandler).
                then(relationshipRecordCreators).
                // then(new PropertyRecordHighIdEventHandler(neoStore.getPropertyStore())).
                then(nodeWriter, relationshipWriter, propertyWriter); //
        RingBuffer<RecordEvent> ringBuffer = incomingEventDisruptor.start();
        long time = System.currentTimeMillis();
        int i = 0;
        boolean outgoing=false;
        try {
            for (; i < ITERATIONS; i++) {
                long sequence = ringBuffer.next();
                RecordEvent recordEvent = ringBuffer.get(sequence);
                // todo data creation takes really a long time !! 20s downto 5s
                // from array creation and Long.valueOf()
                recordEvent.init();
                recordEvent.addProperty(blocked, Boolean.TRUE);
                recordEvent.addProperty(age, VALUE);
                // now only "local" relationships close to the original node-id
                for (int r=0;r<RELS_PER_NODE; r++) {
                    int target = i + r + 1;
                    if (target >= ITERATIONS) continue;
                    recordEvent.addRel(target, outgoing, type).addProperty(weight, WEIGHT);
                    outgoing = !outgoing;
                }
                if (i % (ITERATIONS/10) == 0) System.out.println(i + " "+(System.currentTimeMillis()-time)+" ms.");
                ringBuffer.publish(sequence);
            }
        } finally {
            System.out.println("Iteration " + i);
            incomingEventDisruptor.shutdown();
            executor.shutdown();
            nodeWriter.close();
            relationshipWriter.close();
            inserter.shutdown();
        }
        time = System.currentTimeMillis() - time;
        System.out.println(ITERATIONS + " took " + time + " ms");
        System.out.println("mapped " + Arrays.deepToString(propertyMappingHandlers));

        System.out.println("ids " + idSettingEventHandler);
        System.out.println("relIds " + relIdSettingEventHandler);
        System.out.println("relRecords " + Arrays.deepToString(relationshipRecordCreators));

        System.out.println("wrote nodes " + nodeWriter);
        System.out.println("wrote rels " + relationshipWriter);
        System.out.println("wrote props " + propertyWriter);

        ConsistencyCheckTool.main(new String[]{STORE_DIR});
    }


    public static class Factory implements EventFactory<RecordEvent> {
        @Override
        public RecordEvent newInstance() {
            return new RecordEvent(NODE_PROPERTY_COUNT,RELS_PER_NODE, REL_PROPERTY_COUNT);
        }
    }

    public static class IdSettingEventHandler implements EventHandler<RecordEvent> {
        AtomicLong nodeId = new AtomicLong();

        public void onEvent(RecordEvent event, long sequence, boolean endOfBatch) throws Exception {
            event.id = nodeId.getAndIncrement();
        }

        @Override
        public String toString() {
            return "nodeId: " + nodeId;
        }
    }

    public static class RelIdSettingEventHandler implements EventHandler<RecordEvent> {
        final AtomicLong relId = new AtomicLong();
        // these are rel-id-records where the
        // todo replace by something faster and smaller
        // todo concurrency issue, seems that an potential different thread that executes the RelIdSettingEventHandler
        // won't see the added values in the multi-map (no volatile, final inside there)
        final IntIntMultiMap futureModeRelIdQueueOutgoing = new IntIntMultiMap(RELS_PER_NODE);
        final IntIntMultiMap futureModeRelIdQueueIncoming = new IntIntMultiMap(RELS_PER_NODE);

        public void onEvent(RecordEvent event, long sequence, boolean endOfBatch) throws Exception {
            for (int i = 0; i < event.relationshipCount; i++) {
                Rel relationship = event.relationships[i];
                long id = relId.getAndIncrement();
                relationship.id = id;
                storeFutureRelId(event, relationship,id);
            }

            event.outgoingRelationshipsToUpdate = futureRelIds(event, futureModeRelIdQueueOutgoing);
            event.incomingRelationshipsToUpdate = futureRelIds(event, futureModeRelIdQueueIncoming);
            event.nextRel = firstRelationshipId(event);
            event.maxRelationshipId = maxRelationshipId(event);
        }

        private void storeFutureRelId(RecordEvent event, Rel relationship, long relId) {
            long other = relationship.other();
            if (other <= event.id) return;
            if (relationship.outgoing()) {
                futureModeRelIdQueueIncoming.put((int)other, (int)relId); // todo long vs. int
            } else {
                futureModeRelIdQueueOutgoing.put((int)other, (int)relId); // todo long vs. int
            }
            if (log.isDebugEnabled()) log.debug(event.id+" Adding: "+(int)other+" rel: "+(int)relId+" incoming "+relationship.outgoing());
        }

        private int[] futureRelIds(RecordEvent event, IntIntMultiMap futureRelIds) {
            int[] relIds = futureRelIds.remove((int) event.id);
            if (relIds == null) return null;
            return relIds;
        }

        private long firstRelationshipId(RecordEvent event) {
            if (event.relationshipCount>0) return event.relationships[0].id;
            if (event.outgoingRelationshipsToUpdate!=null) return event.outgoingRelationshipsToUpdate[0];
            if (event.incomingRelationshipsToUpdate!=null) return event.incomingRelationshipsToUpdate[0];
            return Record.NO_PREV_RELATIONSHIP.intValue();
        }
        private long maxRelationshipId(RecordEvent event) {
            long result=Record.NO_NEXT_RELATIONSHIP.intValue();

            if (event.incomingRelationshipsToUpdate!=null) result=Math.max(event.incomingRelationshipsToUpdate[IntIntMultiMap.size(event.incomingRelationshipsToUpdate)-1],result);
            if (event.outgoingRelationshipsToUpdate!=null) result=Math.max(event.outgoingRelationshipsToUpdate[IntIntMultiMap.size(event.outgoingRelationshipsToUpdate)-1],result);
            if (event.relationshipCount>0) result=Math.max(event.relationships[event.relationshipCount-1].id,result);
            return result;
        }

        @Override
        public String toString() {
            return "relId: " + relId;
        }
    }

    static class Property {
        int index;
        Object value;
        PropertyBlock block;

        void init(int index, Object value) {
            this.index=index;
            this.value = value;
            this.block = null;
        }
        void encode(PropertyStore propStore) {
            PropertyBlock block = new PropertyBlock();
            propStore.encodeValue(block, index, value);
            this.block = block;
        }
    }

    public static class PropertyMappingEventHandler implements EventHandler<RecordEvent> {
        private long count;
        private final int pos;
        private final PropertyStore propStore;
        public static final int MASK = 1;

        public PropertyMappingEventHandler(BatchInserterImpl inserter, int pos) {
            this.pos = pos;
            propStore = inserter.getPropertyStore();
        }

        public void onEvent(RecordEvent event, long sequence, boolean endOfBatch) throws Exception {
            if ((sequence & MASK) != pos) return;
            encodeProperties(event);
            for (int i = 0; i < event.relationshipCount; i++) {
                 encodeProperties(event.relationships[i]);
            }
        }

        private void encodeProperties(PropertyHolder holder) {
            if (holder.propertyCount ==0) return;
            // todo cache encoded blocks in an LRU cache
            for (int id = 0; id < holder.propertyCount; id++) {
                Property value = holder.properties[id];
                value.encode(propStore);
                count++;
            }
        }

        @Override
        public String toString() {
            return "encoded "+count+" properties";
        }
    }

    private static class IntIntMultiMap {
        private final ConcurrentHashMap<Integer,int[]> inner=new ConcurrentHashMap<Integer,int[]>();
        private final int arraySize;

        private IntIntMultiMap(int arraySize) {
            this.arraySize = arraySize;
        }

        public void put(int key, int value) {
            int[] ints = inner.get(key);
            if (ints==null) {
                ints = new int[arraySize];
                Arrays.fill(ints,-1);
                inner.putIfAbsent(key, ints);
            }
            for (int i=0;i<arraySize;i++) {
                if (ints[i]==-1) {
                    ints[i]=value;
                    return;
                }
            }
            throw new ArrayIndexOutOfBoundsException("Already "+arraySize+" values in array "+Arrays.toString(ints));
        }

        public int[] remove(int key) {
            return inner.remove(key);
        }

        public static int size(int[] ints) {
            int count = ints.length;
            for (int i=0;i<count;i++) {
                if (ints[i]==-1) return i;
            }
            return count;
        }
    }
    public static class PropertyRecordHighIdEventHandler implements EventHandler<RecordEvent> {
        private final PropertyStore propStore;

        public PropertyRecordHighIdEventHandler(PropertyStore propStore) {
            this.propStore = propStore;
        }

        public void onEvent(RecordEvent event, long sequence, boolean endOfBatch) throws Exception {
            if (propStore.getHighId()<event.lastPropertyId) propStore.setHighId(event.lastPropertyId);
        }
    }

    public static class PropertyRecordEventHandler implements EventHandler<RecordEvent> {
        public static final int PAYLOAD_SIZE = PropertyType.getPayloadSize();
        final AtomicLong propertyId=new AtomicLong();

        @Override
        public void onEvent(RecordEvent event, long sequence, boolean endOfBatch) throws Exception {
            createPropertyRecords(event);
            for (int i = 0; i < event.relationshipCount; i++) {
                createPropertyRecords(event.relationships[i]);
            }
            event.lastPropertyId = propertyId.get();
        }

        private void createPropertyRecords(PropertyHolder holder) {
            if (holder.propertyCount==0) return;
            holder.firstPropertyId = propertyId.get();
            PropertyRecord currentRecord = createRecord(propertyId.incrementAndGet());
            int index=0;
            holder.propertyRecords[index++] = currentRecord;
            for (int i = 0; i < holder.propertyCount; i++) {
                PropertyBlock block = holder.properties[i].block;
                if (currentRecord.size() + block.getSize() > PAYLOAD_SIZE){
                    propertyId.incrementAndGet();
                    currentRecord.setNextProp(propertyId.get());
                    currentRecord = createRecord(propertyId.get());
                    currentRecord.setPrevProp(propertyId.get()-1);
                    holder.propertyRecords[index++] = currentRecord;
                }
                currentRecord.addPropertyBlock(block);
            }
            if (index<holder.propertyRecords.length) holder.propertyRecords[index]=null;
        }

        private PropertyRecord createRecord(long id) {
            PropertyRecord currentRecord = new PropertyRecord(id);
            currentRecord.setInUse( true );
            currentRecord.setCreated();
            return currentRecord;
        }

        @Override
        public String toString() {
            return "MaxPropertyId "+propertyId;
        }
    }


    public static class NodeRecordWritingEventHandler implements EventHandler<RecordEvent> {

        long counter = 0;
        private final NodeStore nodeStore;

        public NodeRecordWritingEventHandler(NodeStore nodeStore) {
            this.nodeStore = nodeStore;
        }

        public void onEvent(RecordEvent event, long sequence, boolean endOfBatch) throws Exception {
            counter++;
            if (nodeStore.getHighId() < event.id) nodeStore.setHighId(event.id+1);
            //printNode(event);
            nodeStore.updateRecord(event.record());
            if (endOfBatch) nodeStore.flushAll();
        }

        @Override
        public String toString() {
            return "WritingEventHandler " + counter;
        }

        public void close() {
            nodeStore.flushAll();
        }
    }

    interface IRelationshipWriter {
        void create(RecordEvent event, Rel rel, long prevId, long nextId);
        void update(long relId, boolean outgoing, long prevId, long nextId);
        void flush();
    }

    public static class RelationshipRecordWriter implements IRelationshipWriter {
        private final RelationshipStore relationshipStore;

        public RelationshipRecordWriter(RelationshipStore relationshipStore) {
            this.relationshipStore = relationshipStore;
        }

        @Override
        public void create(RecordEvent event, Rel rel, long prevId, long nextId) {
            relationshipStore.updateRecord(createRecord(event.id,rel,prevId,nextId));
        }

        @Override
        public void update(long relId, boolean outgoing, long prevId, long nextId) {
            RelationshipRecord record = relationshipStore.getRecord(relId);
            if (outgoing) {
                record.setFirstPrevRel(prevId);
                record.setFirstNextRel(nextId);
            } else {
                record.setSecondPrevRel(prevId);
                record.setSecondNextRel(nextId);
            }
            relationshipStore.updateRecord(record);
        }

        @Override
        public void flush() {
            relationshipStore.flushAll();
        }

    }
    // todo move into class
    private static RelationshipRecord createRecord(long from, Rel rel, long prevId, long nextId) {
        long id = rel.id;
        RelationshipRecord relRecord = rel.outgoing() ?
                    new RelationshipRecord( id, from, rel.other(), rel.type ) :
                    new RelationshipRecord( id, rel.other(), from,  rel.type );
        relRecord.setInUse(true);
        relRecord.setCreated();
        if (rel.outgoing()) {
            relRecord.setFirstPrevRel(prevId);
            relRecord.setFirstNextRel(nextId);
        } else {
            relRecord.setSecondPrevRel(prevId);
            relRecord.setSecondNextRel(nextId);
        }
        relRecord.setNextProp(rel.firstPropertyId);
        return relRecord;
    }

    public static class RelationshipRecordCreator implements EventHandler<RecordEvent> {
        static int MASK = 0;
        private long counter;
        private final int pos;

        public RelationshipRecordCreator(int pos) {
            this.pos = pos;
        }

        @Override
        public void onEvent(RecordEvent event, long sequence, boolean endOfBatch) throws Exception {
            if ((sequence & MASK)!=pos) return;

            if (Record.NO_NEXT_RELATIONSHIP.is(event.nextRel)) return;
            int index=0;
            int count = event.relationshipCount;
            int followingNextRelationshipId =
                    event.outgoingRelationshipsToUpdate!=null ? event.outgoingRelationshipsToUpdate[0] :
                    event.incomingRelationshipsToUpdate!=null ? event.incomingRelationshipsToUpdate[0] :
                                                                Record.NO_NEXT_RELATIONSHIP.intValue();

            long prevId = Record.NO_PREV_RELATIONSHIP.intValue();
            for (int i = 0; i < count; i++) {
                long nextId = i+1 < count ? event.relationships[i + 1].id : followingNextRelationshipId;
                event.relationshipRecords[index++] = createRecord(event.id, event.relationships[i], prevId, nextId);
                prevId = event.relationships[i].id;
                counter++;
            }

            followingNextRelationshipId =
                    event.incomingRelationshipsToUpdate!=null ? event.incomingRelationshipsToUpdate[0] :
                                                                Record.NO_NEXT_RELATIONSHIP.intValue();

            index = createUpdateRecords(event,index, event.outgoingRelationshipsToUpdate, prevId, followingNextRelationshipId,true);

            prevId = event.relationshipRecords[index-1].getId();

            followingNextRelationshipId = Record.NO_NEXT_RELATIONSHIP.intValue();

            index = createUpdateRecords(event,index, event.incomingRelationshipsToUpdate, prevId, followingNextRelationshipId, false);


/*
            if (log.isDebugEnabled()) {
                StringBuilder sb=new StringBuilder("Creating rel-chain for node "+event.id+" ");
                for (int i=0;i<index;i++) {
                    sb.append("\t").append(formatRecord(event.relationshipRecords[i])).append("\n");
                }
                if (event.outgoingRelationshipsToUpdate!=null)
                    sb.append("\tOutgoing").append(Arrays.toString(event.outgoingRelationshipsToUpdate)).append("\n");
                if (event.incomingRelationshipsToUpdate!=null)
                    sb.append("\tIncoming").append(Arrays.toString(event.incomingRelationshipsToUpdate)).append("\n");
                log.debug(sb.toString());
            }
*/
            if (index<event.relationshipRecords.length) event.relationshipRecords[index]=null;
        }

        private int createUpdateRecords(RecordEvent event, int index, int[] relIds, long prevId, int followingNextRelationshipId, boolean outgoing) {
            if (relIds==null) return index;
            int count = IntIntMultiMap.size(relIds);
            for (int i = 0; i < count; i++) {
                long nextId = i+1 < count ? relIds[i + 1] : followingNextRelationshipId;
                event.relationshipRecords[index++] = createUpdateRecord(relIds[i], outgoing, prevId, nextId);
                prevId = relIds[i];
                counter++;
            }
            return index;
        }

        private RelationshipRecord createUpdateRecord(long id, boolean outgoing, long prevId, long nextId) {
            RelationshipRecord relRecord = new RelationshipRecord(id, -1, -1, -1 );
            relRecord.setInUse(true);
            // no setCreated
            if (outgoing) {
                relRecord.setFirstPrevRel(prevId);
                relRecord.setFirstNextRel(nextId);
            } else {
                relRecord.setSecondPrevRel(prevId);
                relRecord.setSecondNextRel(nextId);
            }
            return relRecord;
        }


        @Override
        public String toString() {
            return "rel-record-creator  " + counter;
        }

/*
        private void connect( NodeRecord node, RelationshipRecord rel )
            {
                if ( node.getNextRel() != Record.NO_NEXT_RELATIONSHIP.intValue() )
                {
                    RelationshipRecord nextRel = relationshipStore.getRecord( node.getNextRel() );
                    boolean changed = false;
                    if ( nextRel.getFirstNode() == node.getId() )
                    {
                        nextRel.setFirstPrevRel( rel.getId() );
                        changed = true;
                    }
                    if ( nextRel.getSecondNode() == node.getId() )
                    {
                        nextRel.setSecondPrevRel( rel.getId() );
                        changed = true;
                    }
                    if ( !changed )
                    {
                        throw new InvalidRecordException( node + " dont match " + nextRel );
                    }
                    relationshipStore.updateRecord( nextRel );
                }
            }
*/
    }
    public static class RecordValidator {
        private void validateStartChain(RelationshipRecord record, RecordEvent event) {
            if (Record.NO_NEXT_RELATIONSHIP.is(record.getFirstPrevRel())) {
                validateStartChain(record, event, record.getFirstNode());
            }
            if (Record.NO_NEXT_RELATIONSHIP.is(record.getSecondPrevRel())) {
                validateStartChain(record, event, record.getSecondNode());
            }
        }

        private void validateStartChain(RelationshipRecord record, RecordEvent event, long nodeId) {
            if (nodeId == event.id) {
                if (event.nextRel != record.getId()) {
                    invalidChainHead(record, event);
                }
            }
        }

        private void invalidChainHead(RelationshipRecord record, RecordEvent node) {
            log.error(formatRecord(record));
            log.error(formatNode(node));
            throw new IllegalStateException("Relationship not first in chain of start node");
        }
    }

    public static class RelationshipWriter implements EventHandler<RecordEvent> {
        long counter = 0;
        private final RelationshipStore relationshipStore;
        private RecordValidator validator;

        public RelationshipWriter(RelationshipStore relationshipStore, RecordValidator validator) {
            this.relationshipStore = relationshipStore;
            this.validator = validator;
        }
        // create chain of outgoing relationships
        // todo add incoming relationships ??
        // todo add secondPrev/Next-Rel-id's
        public void onEvent(RecordEvent event, long sequence, boolean endOfBatch) throws Exception {
            if (Record.NO_NEXT_RELATIONSHIP.is(event.nextRel)) return;
            if (relationshipStore.getHighId() < event.maxRelationshipId) relationshipStore.setHighId(event.maxRelationshipId +1);
            int count = event.relationshipRecords.length;
            for (int i = 0; i < count; i++) {
                RelationshipRecord record = event.relationshipRecords[i];
                if (record==null) break;
                try {
                if (record.isCreated()) {
                    // print(record);
                    validator.validateStartChain(record, event);
                    relationshipStore.updateRecord(record);
                }
                else {
                    // TODO write the 2 pointers directly
                    RelationshipRecord loadedRecord = relationshipStore.getRecord(record.getId());
                    // print(loadedRecord);
                    if (!Record.NO_PREV_RELATIONSHIP.is(record.getFirstPrevRel())) loadedRecord.setFirstPrevRel(record.getFirstPrevRel());
                    if (!Record.NO_NEXT_RELATIONSHIP.is(record.getFirstNextRel())) loadedRecord.setFirstNextRel(record.getFirstNextRel());
                    if (!Record.NO_PREV_RELATIONSHIP.is(record.getSecondPrevRel())) loadedRecord.setSecondPrevRel(record.getSecondPrevRel());
                    if (!Record.NO_NEXT_RELATIONSHIP.is(record.getSecondNextRel())) loadedRecord.setSecondNextRel(record.getSecondNextRel());
                    validator.validateStartChain(loadedRecord, event);
                    relationshipStore.updateRecord(loadedRecord);
                }
                } catch(Exception e) {
                    log.error("Error updating relationship-record",e);
                }
                counter++;
            }
            if (endOfBatch) relationshipStore.flushAll();
        }

        @Override
        public String toString() {
            return "rel-record-writer  " + counter;
        }

        public void close() {
            relationshipStore.flushAll();
        }

    }

    private static void printRelationship(RelationshipRecord record) {
        if (log.isDebugEnabled()) log.debug(formatRecord(record));
    }

    private static String formatRecord(RelationshipRecord record) {
        return String.format("Rel[%d] %s-[%d]->%s created %s chain start: %d->%d target %d->%d", record.getId(), record.getFirstNode(), record.getType(), record.getSecondNode(), record.isCreated(), record.getFirstPrevRel(), record.getFirstNextRel(), record.getSecondPrevRel(), record.getSecondNextRel());
    }

    public static class PropertyWriter implements EventHandler<RecordEvent> {

        long counter = 0;
        private final PropertyStore propStore;

        public PropertyWriter(PropertyStore propStore) {
            this.propStore = propStore;
        }

        public void onEvent(RecordEvent event, long sequence, boolean endOfBatch) throws Exception {
            if (propStore.getHighId() < event.lastPropertyId) propStore.setHighId(event.lastPropertyId);
            writePropertyRecords(event);
            for (int i = 0; i < event.relationshipCount; i++) {
                writePropertyRecords(event.relationships[i]);
            }
            if (endOfBatch) propStore.flushAll();
        }

        private boolean writePropertyRecords(PropertyHolder holder) {
            if (holder.propertyCount==0) return true;

            for (int i=0;i<holder.propertyCount;i++) {
                PropertyRecord record = holder.propertyRecords[i];
                if (record == null) return true;
                propStore.updateRecord(record);
                counter++;
            }
            return false;
        }

        @Override
        public String toString() {
            return "PropertyWritingEventHandler " + counter;
        }

    }

    public static class NodeWriter implements EventHandler<RecordEvent> {
        public static final int CAPACITY = (1024 ^ 2)*16;
        FileOutputStream os;
        int eob=0;
        private final FileChannel channel;
        private final ByteBuffer buffer;
        private int limit;
        private long written;

        public NodeWriter(File file) throws FileNotFoundException {
            os = new FileOutputStream(file);
            channel = os.getChannel();
            buffer = ByteBuffer.allocateDirect(CAPACITY);
            limit = ((int)(CAPACITY/9))*9;
            buffer.limit(limit);
        }

        @Override
        public void onEvent(RecordEvent event, long sequence, boolean endOfBatch) throws Exception {
            writeRecord(event);
            if (endOfBatch) {
                channel.force(true);
                eob++;
                //System.out.print(".");
                //if (eob % 100 == 0) System.out.println();
            }
        }

        @Override
        public String toString() {
            return "batches "+eob+" written "+written;
        }

        private void writeRecord(RecordEvent record) throws IOException {
            //printNode(record);
            long nextRel = record.nextRel;
            long nextProp = record.firstPropertyId;

            short relModifier = Record.NO_NEXT_RELATIONSHIP.is(nextRel) ? 0 : (short) ((nextRel & 0x700000000L) >> 31);
            short propModifier = Record.NO_NEXT_PROPERTY.is(nextProp) ? 0 : (short) ((nextProp & 0xF00000000L) >> 28);

            // [    ,   x] in use bit
            // [    ,xxx ] higher bits for rel id
            // [xxxx,    ] higher bits for prop id
            short inUseUnsignedByte = Record.IN_USE.byteValue();
            inUseUnsignedByte = (short) (inUseUnsignedByte | relModifier | propModifier);
            buffer.put((byte)inUseUnsignedByte).putInt((int)nextRel).putInt((int) nextProp);
            if (buffer.position()==buffer.limit()) {
                buffer.position(0);
                written += channel.write(buffer);
                //channel.force(true);
                buffer.clear().limit(limit);
            }
        }

        public void close() throws IOException {
            channel.force(true);
            channel.close();
            os.close();
        }
    }

    private static void printNode(RecordEvent record) {
        if (log.isDebugEnabled()) log.debug(formatNode(record));
    }

    private static String formatNode(RecordEvent record) {
        return String.format("Node[%d] -> %d, .%d", record.id, record.nextRel, record.firstPropertyId);
    }
    private static String formatNode(NodeRecord record) {
        return String.format("Node[%d] -> %d, .%d", record.getId(), record.getNextRel(), record.getNextProp());
    }

    public static class PropertyHolder {
        long id;

        int propertyCount;
        Property[] properties;
        //long q1,q2,q3,q4,q5,q6,q7;
        PropertyRecord[] propertyRecords;
        long firstPropertyId = Record.NO_NEXT_PROPERTY.intValue();
        long q1,q2,q3,q4,q5,q6,q7;
        public PropertyHolder(int propertyCount) {
            this.properties = new Property[propertyCount];
            for (int i = 0; i < properties.length; i++) {
                properties[i]=new Property();
            }
            this.propertyRecords =new PropertyRecord[propertyCount];
        }
        void init() {
            id = 0;
            firstPropertyId = Record.NO_NEXT_PROPERTY.intValue();
            propertyCount = 0;
        }
        public void addProperty(int id, Object value) {
            this.properties[propertyCount++].init(id,value);
        }
    }

    public static class RecordEvent extends PropertyHolder {
        //long p1,p2,p3,p4,p5,p6,p7;
        volatile long nextRel = Record.NO_NEXT_RELATIONSHIP.intValue();
        //long o1,o2,o3,o4,o5,o6,o7;

        Rel[] relationships;
        volatile int relationshipCount;

        volatile long lastPropertyId;
        volatile long maxRelationshipId;
        RelationshipRecord[] relationshipRecords;
        volatile int[] outgoingRelationshipsToUpdate;
        volatile int[] incomingRelationshipsToUpdate;

        public RecordEvent(int propertyCount, int relCount, int relPropertyCount) {
            super(propertyCount);
            this.relationships=new Rel[relCount];
            this.relationshipRecords=new RelationshipRecord[relCount*3]; // potentially as many incoming records
            for (int i = 0; i < relCount; i++) {
                relationships[i]=new Rel(relPropertyCount);
            }
        }

        NodeRecord record() {
            NodeRecord record = new NodeRecord(id, nextRel, firstPropertyId);
            record.setInUse(true);
            record.setCreated();
            return record;
        }

        void init() {
            super.init();
            relationshipCount=0;
            nextRel = Record.NO_NEXT_RELATIONSHIP.intValue();
        }
        Rel addRel(long other, boolean outgoing, int type) {
            return relationships[relationshipCount++].init(other,outgoing,type);
        }
    }

    public static class Rel extends PropertyHolder {
        // encode outgoing > 0, incoming as 2-complement ~other
        volatile long other;
        volatile int type;

        public Rel(int propertyCount) {
            super(propertyCount);
        }

        Rel init(long other, boolean outgoing, int type) {
            super.init();
            this.other = outgoing ? other : ~other;
            this.type = type;
            return this;
        }
        boolean outgoing() {
            return other >= 0;
        }

        long other() {
            return other < 0 ? ~other : other;
        }

        @Override
        public String toString() {
            return String.format("Rel[%d] %s-[%d]->%s %s",id, outgoing() ? "?" : other(),type,outgoing() ? other() : "?",outgoing());
        }
    }
}