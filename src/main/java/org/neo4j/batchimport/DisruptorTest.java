package org.neo4j.batchimport;

import com.lmax.disruptor.*;
import com.lmax.disruptor.dsl.Disruptor;
import edu.ucla.sspace.util.primitive.IntIntHashMultiMap;
import edu.ucla.sspace.util.primitive.IntIntMultiMap;
import edu.ucla.sspace.util.primitive.IntSet;
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
    public static final int ITERATIONS = 1000*1000;

    @SuppressWarnings("unchecked")
    public static void main(String[] args) throws Exception {
        FileUtils.deleteRecursively(new File(STORE_DIR));
        BatchInserterImpl inserter = (BatchInserterImpl) BatchInserters.inserter(STORE_DIR, stringMap("use_memory_mapped_buffers", "true",
                //"dump_configuration", "true",
                "cache_type", "none",
                "neostore.nodestore.db.mapped_memory", "50M",
                "neostore.propertystore.db.mapped_memory", "1G",
                "neostore.relationshipstore.db.mapped_memory", "500M"
        ));
        NeoStore neoStore = inserter.getNeoStore();
        NodeStore nodeStore = neoStore.getNodeStore();
        nodeStore.setHighId(ITERATIONS + 1);
        final ExecutorService executor = Executors.newFixedThreadPool(Runtime.getRuntime().availableProcessors());
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
        RelIdSettingEventHandler relIdSettingEventHandler = new RelIdSettingEventHandler();

        //NodeWriter nodeWriter = new NodeWriter(new File(nodeStore.getStorageFileName()));
        NodeRecordWritingEventHandler nodeWriter = new NodeRecordWritingEventHandler(nodeStore);
        PropertyWriter propertyWriter = new PropertyWriter(neoStore.getPropertyStore());
        RelationshipWriteHandler relationshipWriter = new RelationshipWriteHandler(new RelationshipRecordWriter(neoStore.getRelationshipStore()));
        //RelationshipWriteHandler relationshipWriter = new RelationshipWriteHandler(new RelationshipFileWriter(new File(neoStore.getRelationshipStore().getStorageFileName())));
        incomingEventDisruptor.
                handleEventsWith(propertyMappingHandlers[0], propertyMappingHandlers[1], idSettingEventHandler).
                then(new PropertyRecordEventHandler(), relIdSettingEventHandler).
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

        System.out.println("wrote nodes " + nodeWriter);
        System.out.println("wrote rels " + relationshipWriter);
        System.out.println("wrote props " + propertyWriter);

        // if (true) ConsistencyCheckTool.main(new String[]{STORE_DIR});
    }


    public static class Factory implements EventFactory<RecordEvent> {
        @Override
        public RecordEvent newInstance() {
            return new RecordEvent(NODE_PROPERTY_COUNT,RELS_PER_NODE, REL_PROPERTY_COUNT);
        }
    }

    public static class IdSettingEventHandler implements EventHandler<RecordEvent> {
        volatile long nodeId = 0;

        public void onEvent(RecordEvent event, long sequence, boolean endOfBatch) throws Exception {
            event.id = nodeId++;
        }

        @Override
        public String toString() {
            return "nodeId: " + nodeId;
        }
    }

    public static class RelIdSettingEventHandler implements EventHandler<RecordEvent> {
        volatile long relId = 0;
        // these are rel-id-records where the
        // todo replace by something faster and smaller
        // todo concurrency issue, seems that an potential different thread that executes the RelIdSettingEventHandler
        // won't see the added values in the multi-map (no volatile, final inside there)
        final ReverseRelationshipMap futureModeRelIdQueueOutgoing = new PrimitiveReverseRelationshipMap();
        final ReverseRelationshipMap futureModeRelIdQueueIncoming = new PrimitiveReverseRelationshipMap();
        //final ReverseRelationshipMap futureModeRelIdQueueOutgoing = new ConcurrentReverseRelationshipMap(RELS_PER_NODE);
        //final ReverseRelationshipMap futureModeRelIdQueueIncoming = new ConcurrentReverseRelationshipMap(RELS_PER_NODE);

        public void onEvent(RecordEvent event, long sequence, boolean endOfBatch) throws Exception {
            for (int i = 0; i < event.relationshipCount; i++) {
                Rel relationship = event.relationships[i];
                long id = relId++;
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
                futureModeRelIdQueueIncoming.add((int) other, (int) relId); // todo long vs. int
            } else {
                futureModeRelIdQueueOutgoing.add((int) other, (int) relId); // todo long vs. int
            }
            if (log.isDebugEnabled()) log.debug(event.id+" Adding: "+(int)other+" rel: "+(int)relId+" incoming "+relationship.outgoing());
        }

        private int[] futureRelIds(RecordEvent event, ReverseRelationshipMap futureRelIds) {
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

            if (event.incomingRelationshipsToUpdate!=null) result=Math.max(event.incomingRelationshipsToUpdate[size(event.incomingRelationshipsToUpdate)-1],result);
            if (event.outgoingRelationshipsToUpdate!=null) result=Math.max(event.outgoingRelationshipsToUpdate[size(event.outgoingRelationshipsToUpdate)-1],result);
            if (event.relationshipCount>0) result=Math.max(event.relationships[event.relationshipCount-1].id,result);
            return result;
        }

        @Override
        public String toString() {
            return "relId: " + relId;
        }
    }

    static class Property {
        volatile int index;
        volatile Object value;
        volatile PropertyBlock block;

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

    interface ReverseRelationshipMap {
         void add(int nodeId, int relId);
         int[] remove(int nodeId);
    }

    private static class PrimitiveReverseRelationshipMap implements ReverseRelationshipMap {
        private final IntIntMultiMap inner=new IntIntHashMultiMap();

        public void add(int key, int value) {
            inner.put(key,value);
        }

        public int[] remove(int key) {
            IntSet relIds = inner.remove(key);
            if (relIds==null) return null;
            return relIds.toPrimitiveArray();
        }
    }

    private static class ConcurrentReverseRelationshipMap implements ReverseRelationshipMap {
        private final ConcurrentHashMap<Integer,int[]> inner=new ConcurrentHashMap<Integer,int[]>();
        private final int arraySize;

        private ConcurrentReverseRelationshipMap(int arraySize) {
            this.arraySize = arraySize;
        }

        public void add(int key, int value) {
            int[] ints = inner.get(key);
            if (ints==null) {
                ints = new int[arraySize];
                Arrays.fill(ints,-1);
                inner.put(key, ints);
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

    }
    public static int size(int[] ints) {
        if (ints==null) return 0;
        int count = ints.length;
        for (int i=count-1;i>=0;i--) {
            if (ints[i]!=-1) return i+1;
        }
        return count;
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
        volatile long propertyId=0;

        @Override
        public void onEvent(RecordEvent event, long sequence, boolean endOfBatch) throws Exception {
            createPropertyRecords(event);
            for (int i = 0; i < event.relationshipCount; i++) {
                createPropertyRecords(event.relationships[i]);
            }
            event.lastPropertyId = propertyId;
        }

        private void createPropertyRecords(PropertyHolder holder) {
            if (holder.propertyCount==0) return;
            holder.firstPropertyId = propertyId++;
            PropertyRecord currentRecord = createRecord(propertyId);
            int index=0;
            holder.propertyRecords[index++] = currentRecord;
            for (int i = 0; i < holder.propertyCount; i++) {
                PropertyBlock block = holder.properties[i].block;
                if (currentRecord.size() + block.getSize() > PAYLOAD_SIZE){
                    propertyId++;
                    currentRecord.setNextProp(propertyId);
                    currentRecord = createRecord(propertyId);
                    currentRecord.setPrevProp(propertyId-1);
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
            // if (endOfBatch) nodeStore.flushAll();
        }

        @Override
        public String toString() {
            return "WritingEventHandler " + counter;
        }

        public void close() {
            nodeStore.flushAll();
        }
    }

    interface RelationshipWriter {
        void create(RecordEvent event, Rel rel, long prevId, long nextId) throws IOException;
        void update(long relId, boolean outgoing, long prevId, long nextId) throws IOException;
        void flush() throws IOException;

        void start(long maxRelationshipId);

        void close() throws IOException;
    }

    public static class RelationshipRecordWriter implements RelationshipWriter {
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

        @Override
        public void start(long maxRelationshipId) {
            if (relationshipStore.getHighId() < maxRelationshipId) relationshipStore.setHighId(maxRelationshipId +1);
        }

        @Override
        public void close() throws IOException {
            flush();
        }

        private RelationshipRecord createRecord(long from, Rel rel, long prevId, long nextId) {
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
    }

    public static class RelationshipWriteHandler implements EventHandler<RecordEvent> {
        private long counter;
        private final RelationshipWriter relationshipWriter;

        public RelationshipWriteHandler(RelationshipWriter relationshipWriter) {
            this.relationshipWriter = relationshipWriter;
        }

        @Override
        public void onEvent(RecordEvent event, long sequence, boolean endOfBatch) throws Exception {
            if (Record.NO_NEXT_RELATIONSHIP.is(event.nextRel)) return;
            relationshipWriter.start(event.maxRelationshipId);

            int count = event.relationshipCount;
            int followingNextRelationshipId =
                    event.outgoingRelationshipsToUpdate!=null ? event.outgoingRelationshipsToUpdate[0] :
                    event.incomingRelationshipsToUpdate!=null ? event.incomingRelationshipsToUpdate[0] :
                                                                Record.NO_NEXT_RELATIONSHIP.intValue();

            long prevId = Record.NO_PREV_RELATIONSHIP.intValue();
            for (int i = 0; i < count; i++) {
                long nextId = i+1 < count ? event.relationships[i + 1].id : followingNextRelationshipId;
                Rel rel = event.relationships[i];
                relationshipWriter.create(event, rel, prevId, nextId);
                prevId = rel.id;
                counter++;
            }

            followingNextRelationshipId =
                    event.incomingRelationshipsToUpdate!=null ? event.incomingRelationshipsToUpdate[0] :
                                                                Record.NO_NEXT_RELATIONSHIP.intValue();

            prevId = createUpdateRecords(event.outgoingRelationshipsToUpdate, prevId, followingNextRelationshipId,true);

            followingNextRelationshipId = Record.NO_NEXT_RELATIONSHIP.intValue();

            createUpdateRecords(event.incomingRelationshipsToUpdate, prevId, followingNextRelationshipId, false);

            // if (endOfBatch) relationshipWriter.flush();
        }

        private long createUpdateRecords(int[] relIds, long prevId, int followingNextRelationshipId, boolean outgoing) throws IOException {
            if (relIds==null) return prevId;
            int count = size(relIds);
            for (int i = 0; i < count; i++) {
                long nextId = i+1 < count ? relIds[i + 1] : followingNextRelationshipId;
                relationshipWriter.update(relIds[i], outgoing, prevId, nextId);
                prevId = relIds[i];
                counter++;
            }
            return prevId;
        }

        @Override
        public String toString() {
            return "rel-record-writer  " + counter + " "+relationshipWriter;
        }
        public void close() throws IOException {
            relationshipWriter.close();
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
    public static class RelationshipFileWriter implements RelationshipWriter {
        public static final int CAPACITY = (1024 ^ 2);
        FileOutputStream os;
        int eob=0;
        private final FileChannel channel;
        private final ByteBuffer buffer;
        private int limit;
        private long written;
        private ByteBuffer updateBuffer;
        private long updated;

        public RelationshipFileWriter(File file) throws IOException {
            os = new FileOutputStream(file);
            channel = os.getChannel();
            channel.position(0);
            buffer = ByteBuffer.allocateDirect(CAPACITY);
            updateBuffer = ByteBuffer.allocateDirect(8); // 2x prev/next pointer
            limit = ((int)(CAPACITY/RelationshipStore.RECORD_SIZE))*RelationshipStore.RECORD_SIZE;
            buffer.limit(limit);
        }

        @Override
        public void create(RecordEvent event, Rel rel, long prevId, long nextId) throws IOException {
            long from = event.id;
            long id = rel.id;

            long firstNode, secondNode, firstNextRel, firstPrevRel, secondNextRel, secondPrevRel;

            if (rel.outgoing()) {
                firstNode = from;
                secondNode = rel.other();
                firstPrevRel = prevId;
                firstNextRel = nextId;
                secondPrevRel = Record.NO_PREV_RELATIONSHIP.intValue();
                secondNextRel = Record.NO_NEXT_RELATIONSHIP.intValue();
            } else {
                firstNode = rel.other();
                secondNode = from;
                firstPrevRel = Record.NO_PREV_RELATIONSHIP.intValue();
                firstNextRel = Record.NO_NEXT_RELATIONSHIP.intValue();
                secondPrevRel = prevId;
                secondNextRel = nextId;
            }

            short firstNodeMod = (short)((firstNode & 0x700000000L) >> 31);
            long secondNodeMod = (secondNode & 0x700000000L) >> 4;
            long firstPrevRelMod = firstPrevRel == Record.NO_NEXT_RELATIONSHIP.intValue() ? 0 : (firstPrevRel & 0x700000000L) >> 7;
            long firstNextRelMod = firstNextRel == Record.NO_NEXT_RELATIONSHIP.intValue() ? 0 : (firstNextRel & 0x700000000L) >> 10;
            long secondPrevRelMod = secondPrevRel == Record.NO_NEXT_RELATIONSHIP.intValue() ? 0 : (secondPrevRel & 0x700000000L) >> 13;
            long secondNextRelMod = secondNextRel == Record.NO_NEXT_RELATIONSHIP.intValue() ? 0 : (secondNextRel & 0x700000000L) >> 16;

            long nextProp = rel.firstPropertyId;
            long nextPropMod = nextProp == Record.NO_NEXT_PROPERTY.intValue() ? 0 : (nextProp & 0xF00000000L) >> 28;

            // [    ,   x] in use flag
            // [    ,xxx ] first node high order bits
            // [xxxx,    ] next prop high order bits
            short inUseUnsignedByte = (short)(Record.IN_USE.byteValue() | firstNodeMod | nextPropMod);

            // [ xxx,    ][    ,    ][    ,    ][    ,    ] second node high order bits,     0x70000000
            // [    ,xxx ][    ,    ][    ,    ][    ,    ] first prev rel high order bits,  0xE000000
            // [    ,   x][xx  ,    ][    ,    ][    ,    ] first next rel high order bits,  0x1C00000
            // [    ,    ][  xx,x   ][    ,    ][    ,    ] second prev rel high order bits, 0x380000
            // [    ,    ][    , xxx][    ,    ][    ,    ] second next rel high order bits, 0x70000
            // [    ,    ][    ,    ][xxxx,xxxx][xxxx,xxxx] type
            int typeInt = (int)(rel.type | secondNodeMod | firstPrevRelMod | firstNextRelMod | secondPrevRelMod | secondNextRelMod);

            buffer.put( (byte)inUseUnsignedByte ).putInt( (int) firstNode ).putInt((int) secondNode)
                  .putInt(typeInt).putInt( (int) firstPrevRel ).putInt( (int) firstNextRel )
                  .putInt((int) secondPrevRel).putInt( (int) secondNextRel ).putInt( (int) nextProp );

            flushBuffer(false);
        }

        private void flushBuffer(boolean force) throws IOException {
            if (buffer.position()==0) return;
            if (force || buffer.position()==buffer.limit()) {
                long position = channel.position();
                buffer.limit(buffer.position());
                buffer.position(0);
                int wrote = channel.write(buffer);
                written += wrote;
                System.out.println("RelStore: at "+ position +" wrote "+wrote+" force "+force);
                buffer.clear().limit(limit);
            }
        }

        /**
         * only works for prevId & nextId <= MAXINT
         */
        @Override
        public void update(long id, boolean outgoing, long prevId, long nextId) throws IOException {
            flushBuffer(true);
            long position = id * RelationshipStore.RECORD_SIZE + 1 + 4 + 4 + 4; // inUse, firstNode, secondNode, relType

            if (!outgoing) {
                position += 4 + 4;
            }
            long oldPos = channel.position();
            if (oldPos != position) {
                channel.position(position);
            }

            updateBuffer.position(0);
            updateBuffer.putInt((int) prevId).putInt( (int) nextId ).position(0);

            int wrote = channel.write(updateBuffer);
            updated += wrote;
            System.out.println("RelStore: at "+ position +" oldPos " +oldPos+" update "+wrote+" id "+id+" outgoing "+outgoing+" prevId "+prevId+" nextId "+nextId);
            channel.position(oldPos);
        }

        @Override
        public void close() throws IOException {
            flush();
            channel.close();
            os.close();
        }

        @Override
        public void flush() throws IOException {
            flushBuffer(true);
            eob++;
            channel.force(true);
        }

        @Override
        public void start(long maxRelationshipId) {
        }
        @Override
        public String toString() {
            return "RelationshipFileWriter: batches "+eob+" written "+written+" updated "+updated;
        }
    }

    public static class NodeWriter implements EventHandler<RecordEvent> {
        public static final int CAPACITY = (1024 ^ 2);
        FileOutputStream os;
        int eob=0;
        private final FileChannel channel;
        private final ByteBuffer buffer;
        private int limit;
        private long written;

        public NodeWriter(File file) throws IOException {
            os = new FileOutputStream(file);
            channel = os.getChannel();
            channel.position(0);
            buffer = ByteBuffer.allocateDirect(CAPACITY);
            limit = ((int)(CAPACITY/9))*9;
            buffer.limit(limit);
        }

        @Override
        public void onEvent(RecordEvent event, long sequence, boolean endOfBatch) throws Exception {
            writeRecord(event);
            if (endOfBatch) {
                flush();
            }
        }

        private void flush() throws IOException {
            flushBuffer(true);
            channel.force(true);
            eob++;
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
            flushBuffer(false);
        }

        private void flushBuffer(boolean force) throws IOException {
            if (buffer.position()==0) return;
            if (force || buffer.position()==buffer.limit()) {
                buffer.limit(buffer.position());
                buffer.position(0);
                // long position = channel.position();
                int wrote = channel.write(buffer);
                written += wrote;
                // System.out.println("NodeStore: at "+ position +" wrote "+wrote);
                buffer.clear().limit(limit);
            }
        }

        public void close() throws IOException {
            flush();
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
        volatile long id;
        volatile long firstPropertyId = Record.NO_NEXT_PROPERTY.intValue();

        volatile int propertyCount;
        final Property[] properties;
        final PropertyRecord[] propertyRecords;

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

        final Rel[] relationships;
        volatile int relationshipCount;

        volatile long lastPropertyId;
        volatile long maxRelationshipId;
        volatile int[] outgoingRelationshipsToUpdate;
        volatile int[] incomingRelationshipsToUpdate;

        public RecordEvent(int propertyCount, int relCount, int relPropertyCount) {
            super(propertyCount);
            this.relationships=new Rel[relCount];
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