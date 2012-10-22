package org.neo4j.batchimport;

import com.lmax.disruptor.*;
import com.lmax.disruptor.dsl.Disruptor;
import edu.ucla.sspace.util.primitive.IntIntHashMultiMap;
import edu.ucla.sspace.util.primitive.IntIntMultiMap;
import edu.ucla.sspace.util.primitive.IntSet;
import org.neo4j.kernel.impl.nioneo.store.*;
import org.neo4j.kernel.impl.util.FileUtils;
import org.neo4j.unsafe.batchinsert.BatchInserterImpl;
import org.neo4j.unsafe.batchinsert.BatchInserters;

import java.io.*;
import java.lang.reflect.Field;
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;
import java.util.Arrays;
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
        Field field = BatchInserterImpl.class.getDeclaredField("neoStore");
        field.setAccessible(true);
        NeoStore neoStore = (NeoStore) field.get(inserter);
        NodeStore nodeStore = neoStore.getNodeStore();
        nodeStore.setHighId(ITERATIONS + 1);
        final ExecutorService executor = Executors.newCachedThreadPool();//Runtime.getRuntime().availableProcessors());

        int maxPropertyId = inserter.createAllPropertyIndexes(asList("blocked", "age"));
        int maxRelTypeId = inserter.createAllRelTypeIndexes(asList("weight"));
        int blocked = inserter.getPropertyKeyId("blocked");
        int age = inserter.getPropertyKeyId("age");
        int weight = inserter.getPropertyKeyId("weight");
        int type = inserter.getRelTypeId("CONNECTS");
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

        NodeWriter nodeWriter = new NodeWriter(new File(nodeStore.getStorageFileName()));
        PropertyWriter propertyWriter = new PropertyWriter(neoStore.getPropertyStore());
        RelationshipWriter relationshipWriter = new RelationshipWriter(neoStore.getRelationshipStore());
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
                    recordEvent.addRel(i+r+1,outgoing,type).addProperty(weight, WEIGHT);
                    outgoing = !outgoing;
                }
                if (i % (ITERATIONS/10) == 0) System.out.println(i + " "+(System.currentTimeMillis()-time)+" ms.");
                ringBuffer.publish(sequence);
            }
        } finally {
            System.out.println("Iteration " + i);
            executor.shutdown();
            incomingEventDisruptor.shutdown();
            inserter.shutdown();
            nodeWriter.close();
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
    }


    public static class Factory implements EventFactory<RecordEvent> {
        @Override
        public RecordEvent newInstance() {
            return new RecordEvent(NODE_PROPERTY_COUNT,RELS_PER_NODE, REL_PROPERTY_COUNT);
        }
    }

    public static class IdSettingEventHandler implements EventHandler<RecordEvent> {
        long nodeId = 0;

        public void onEvent(RecordEvent event, long sequence, boolean endOfBatch) throws Exception {
            event.id = nodeId;
            nodeId++;
        }

        @Override
        public String toString() {
            return "nodeId: " + nodeId;
        }
    }

    public static class RelIdSettingEventHandler implements EventHandler<RecordEvent> {
        long relId = 0;
        // these are rel-id-records where the
        // todo replace by something faster and smaller
        IntIntMultiMap futureModeRelIdQueueOutgoing = new IntIntHashMultiMap();
        IntIntMultiMap futureModeRelIdQueueIncoming = new IntIntHashMultiMap();

        public void onEvent(RecordEvent event, long sequence, boolean endOfBatch) throws Exception {
            for (int i = 0; i < event.relationshipCount; i++) {
                Rel relationship = event.relationships[i];
                relationship.id = relId;
                storeFutureRelId(event, relationship,relId);
                relId++;
            }

            event.outgoingRelationshipsToUpdate = futureRelIds(event, futureModeRelIdQueueOutgoing);
            event.incomingRelationshipsToUpdate = futureRelIds(event, futureModeRelIdQueueIncoming);
            event.nextRel = firstRelationshipId(event);
            event.maxRelationshipId = maxRelationshipId(event);
            if (event.maxRelationshipId ==0) {
                System.out.println(event);
            }
        }

        private void storeFutureRelId(RecordEvent event, Rel relationship, long relId) {
            long other = relationship.other();
            if (other <= event.id) return;
            if (relationship.outgoing())
             futureModeRelIdQueueOutgoing.put((int)other, (int)relId); // todo long vs. int
            else
             futureModeRelIdQueueIncoming.put((int)other, (int)relId); // todo long vs. int
        }

        private int[] futureRelIds(RecordEvent event, IntIntMultiMap futureRelIds) {
            IntSet relIds = futureRelIds.remove((int) event.id);
            if (relIds == null || relIds.isEmpty()) return null;
            return relIds.toPrimitiveArray();
        }

        private long firstRelationshipId(RecordEvent event) {
            if (event.relationshipCount>0) return event.relationships[0].id;
            if (event.outgoingRelationshipsToUpdate!=null) return event.outgoingRelationshipsToUpdate[0];
            if (event.incomingRelationshipsToUpdate!=null) return event.incomingRelationshipsToUpdate[0];
            return Record.NO_PREV_RELATIONSHIP.intValue();
        }
        private long maxRelationshipId(RecordEvent event) {
            long result=Record.NO_NEXT_RELATIONSHIP.intValue();
            if (event.incomingRelationshipsToUpdate!=null) result=Math.max(event.incomingRelationshipsToUpdate[event.incomingRelationshipsToUpdate.length-1],result);
            if (event.outgoingRelationshipsToUpdate!=null) result=Math.max(event.outgoingRelationshipsToUpdate[event.outgoingRelationshipsToUpdate.length-1],result);
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
        public static final int MASK = 3;

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
        long propertyId=0;

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
            holder.firstPropertyId = propertyId;
            PropertyRecord currentRecord = createRecord(propertyId++);
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

        static int MASK = 5;
        long counter = 0;
        private final NodeStore nodeStore;
        private final int pos;

        public NodeRecordWritingEventHandler(NodeStore nodeStore, int pos) {
            this.nodeStore = nodeStore;
            this.pos = pos;
        }

        public void onEvent(RecordEvent event, long sequence, boolean endOfBatch) throws Exception {
            if ((sequence & MASK) != pos) return;
            counter++;
            //if (nodeStore.getHighId() < event.nodeId) nodeStore.setHighId(event.nodeId+1);
            nodeStore.updateRecord(event.record());
            if (endOfBatch) nodeStore.flushAll();
        }

        @Override
        public String toString() {
            return "WritingEventHandler " + counter;
        }

    }
    public static class RelationshipRecordCreator implements EventHandler<RecordEvent> {
        static int MASK = 3;
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

            if (index<event.relationshipRecords.length) event.relationshipRecords[index]=null;
        }

        private int createUpdateRecords(RecordEvent event, int index, int[] relIds, long prevId, int followingNextRelationshipId, boolean outgoing) {
            if (relIds==null || relIds.length==0) return index;
            int count = relIds.length;
            for (int i = 0; i < count; i++) {
                long nextId = i+1 < count ? relIds[i + 1] : followingNextRelationshipId;
                event.relationshipRecords[index++] = createUpdateRecord(relIds[i], outgoing, prevId, nextId);
                prevId = relIds[i];
                counter++;
            }
            return index;
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
                relRecord.setFirstPrevRel(prevId);
                relRecord.setSecondNextRel(nextId);
            }
            relRecord.setNextProp(rel.firstPropertyId);
            return relRecord;
        }

        private RelationshipRecord createUpdateRecord(long id, boolean outgoing, long prevId, long nextId) {
            RelationshipRecord relRecord = new RelationshipRecord(id, -1, -1, -1 );
            relRecord.setInUse(true);
            // no setCreated
            if (outgoing) {
                relRecord.setFirstPrevRel(prevId);
                relRecord.setFirstNextRel(nextId);
            } else {
                relRecord.setFirstPrevRel(prevId);
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
    public static class RelationshipWriter implements EventHandler<RecordEvent> {
        long counter = 0;
        private final RelationshipStore relationshipStore;

        public RelationshipWriter(RelationshipStore relationshipStore) {
            this.relationshipStore = relationshipStore;
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
                if (record.isCreated()) relationshipStore.updateRecord(record);
                else {
                    // TODO write the 2 pointers directly
                    RelationshipRecord loadedRecord = relationshipStore.getRecord(record.getId());
                    if (!Record.NO_PREV_RELATIONSHIP.is(record.getFirstPrevRel())) loadedRecord.setFirstPrevRel(record.getFirstPrevRel());
                    if (!Record.NO_NEXT_RELATIONSHIP.is(record.getFirstNextRel())) loadedRecord.setFirstNextRel(record.getFirstNextRel());
                    if (!Record.NO_PREV_RELATIONSHIP.is(record.getSecondPrevRel())) loadedRecord.setSecondPrevRel(record.getSecondPrevRel());
                    if (!Record.NO_NEXT_RELATIONSHIP.is(record.getSecondNextRel())) loadedRecord.setSecondNextRel(record.getSecondNextRel());
                    relationshipStore.updateRecord(record);
                }
                } catch(Exception e) {
                    e.printStackTrace();
                }
                counter++;
            }
            if (endOfBatch) relationshipStore.flushAll();
        }

        @Override
        public String toString() {
            return "rel-record-writer  " + counter;
        }

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
            channel.close();
            os.close();
        }
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
        long nextRel = Record.NO_NEXT_RELATIONSHIP.intValue();
        //long o1,o2,o3,o4,o5,o6,o7;

        Rel[] relationships;
        int relationshipCount;

        long lastPropertyId;
        long maxRelationshipId;
        public RelationshipRecord[] relationshipRecords;
        public int[] outgoingRelationshipsToUpdate;
        public int[] incomingRelationshipsToUpdate;

        public RecordEvent(int propertyCount, int relCount, int relPropertyCount) {
            super(propertyCount);
            this.relationships=new Rel[relCount];
            this.relationshipRecords=new RelationshipRecord[relCount*2]; // potentially as many incoming records
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

    static class Rel extends PropertyHolder {
        // encode outgoing > 0, incoming as 2-complement ~other
        long other;
        int type;

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
            return other > 0;
        }

        long other() {
            return other < 0 ? ~other : other;
        }
    }
}