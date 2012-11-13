package org.neo4j.batchimport;

import org.apache.log4j.Logger;
import org.junit.Ignore;
import org.neo4j.batchimport.structs.NodeStruct;
import org.neo4j.consistency.ConsistencyCheckTool;
import org.neo4j.helpers.collection.MapUtil;
import org.neo4j.kernel.impl.util.FileUtils;
import org.neo4j.unsafe.batchinsert.BatchInserterImpl;

import java.io.*;
import java.util.Map;

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

// two phase approach:
// store first non-own rel-id for node-record in a suitable structure (it is actually a queue that can be emptied from the beginning)
// start with a CHM
// collect per node: last real rel-id==prevId, each of the "to be updated" rel-id's w/ direction & typeInt
// convert that into a sorted file (by rel-id, or block based) of: typeInt, prevId, nextId, direction
// write it in parallel to the rel-file using direct offsets

@Ignore
public class DisruptorTest {

    private final static Logger log = Logger.getLogger(DisruptorBatchInserter.class);

    public static final String STORE_DIR = "target/test-db2";
    public static final int NODES_TO_CREATE = 20 * 1000 * 1000;
    private static final boolean RUN_CHECK = false;
    private static final File PROP_FILE = new File("batch.properties");

    @SuppressWarnings("unchecked")
    public static void main(String[] args) throws Exception {
        FileUtils.deleteRecursively(new File(STORE_DIR));
        final DisruptorBatchInserter inserter = new DisruptorBatchInserter(STORE_DIR, config(), NODES_TO_CREATE, new TestNodeStructFactory(NODES_TO_CREATE));
        inserter.init();
        long time = System.currentTimeMillis();
        try {
            inserter.run();
        } finally {
            inserter.shutdown();
        }
        time = System.currentTimeMillis() - time;
        log.info(NODES_TO_CREATE + " took " + time + " ms");
        inserter.report();

        if (RUN_CHECK) ConsistencyCheckTool.main(new String[]{STORE_DIR});
    }

    private static Map<String, String> config() {
        if (PROP_FILE.exists()) {
            try {
                return MapUtil.load(PROP_FILE);
            } catch (IOException e) {
                throw new RuntimeException(e);
            }
        }
        return stringMap("use_memory_mapped_buffers", "true",
        //"dump_configuration", "true",
        "cache_type", "none",
        "neostore.nodestore.db.mapped_memory", "2G",
        "neostore.propertystore.db.mapped_memory", "5G",
        "neostore.relationshipstore.db.mapped_memory", "20G",
        "neostore.propertystore.db.strings.mapped_memory","2G"
);
    }

    public static class TestNodeStructFactory implements NodeStructFactory {
        public static final int RELS_PER_NODE = 10;
        public static final int MAX_RELS_PER_NODE = TestNodeStructFactory.RELS_PER_NODE;
        public static final int REL_PROPERTY_COUNT = 1;
        public static final int NODE_PROPERTY_COUNT = 2;
        public static final int[] REL_OFFSETS = new int[MAX_RELS_PER_NODE];

        // constant values, to avoid boxing every time
        private final static Float WEIGHT = 10F;
        private final static Long VALUE = 42L;

        boolean outgoing = false;
        private int blocked;
        private int age;
        private int weight;
        private int type;
        private final long nodesToCreate;

        static {
            for (int i = 0; i < MAX_RELS_PER_NODE; i++) TestNodeStructFactory.REL_OFFSETS[i] = 1 << 2 * i;
        }

        public TestNodeStructFactory(long nodesToCreate) {
            this.nodesToCreate = nodesToCreate;
        }

        @Override
        public NodeStruct newInstance() {
            return new NodeStruct(NODE_PROPERTY_COUNT);
        }

        @Override
        public void init(BatchInserterImpl inserter) {
            inserter.createAllPropertyIndexes(asList("blocked", "age", "weight"));
            inserter.createAllRelTypeIndexes(asList("CONNECTS"));

            blocked = inserter.getPropertyKeyId("blocked");
            age = inserter.getPropertyKeyId("age");
            weight = inserter.getPropertyKeyId("weight");
            type = inserter.getRelTypeId("CONNECTS");
            NodeStruct.classInit(RELS_PER_NODE,REL_PROPERTY_COUNT);
        }

        @Override
        public void fillStruct(long nodeId, NodeStruct nodeStruct) {
            // todo data creation takes really a long time !! 20s downto 5s
            // from array creation and Long.valueOf()
            nodeStruct.addProperty(blocked, Boolean.TRUE);
            nodeStruct.addProperty(age, VALUE);
            // now only "local" relationships close to the original node-id
            for (int r = 0; r < MAX_RELS_PER_NODE; r++) {
                long target = nodeId + TestNodeStructFactory.REL_OFFSETS[r];
                // only target nodes beyond the current one
                if (target >= NODES_TO_CREATE) continue;
                nodeStruct.addRel(target, outgoing, type).addProperty(weight, WEIGHT);
                outgoing = !outgoing;
            }
        }

        @Override
        public int getRelsPerNode() {
            return RELS_PER_NODE;
        }

        @Override
        public int getMaxRelsPerNode() {
            return MAX_RELS_PER_NODE;
        }

        public long getTotalNrOfRels() {
            return getRelsPerNode() * nodesToCreate;
        }
    }
}