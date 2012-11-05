package org.neo4j.batchimport;

import org.apache.log4j.Logger;
import org.neo4j.batchimport.importer.RowData;
import org.neo4j.batchimport.structs.NodeStruct;
import org.neo4j.batchimport.structs.PropertyHolder;
import org.neo4j.batchimport.structs.Relationship;
import org.neo4j.batchimport.utils.Params;
import org.neo4j.consistency.ConsistencyCheckTool;
import org.neo4j.kernel.impl.util.FileUtils;
import org.neo4j.unsafe.batchinsert.BatchInserterImpl;

import java.io.*;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;

import static java.util.Arrays.asList;
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

// todo class for import information
// todo better row parsing / handling, record-objects for nodes/rels (at least flyweights - from Node-Struct, PropertyHolder)

public class ParallelImporter implements NodeStructFactory {

    private final static Logger log = Logger.getLogger(ParallelImporter.class);

    private static final boolean RUN_CHECK = false;
    private static final int MEGABYTE = 1024 * 1024;
    private DisruptorBatchInserter inserter;
    private final File graphDb;
    private final File nodesFile;
    private final File relationshipsFile;
    private final long nodesToCreate;
    private final int propsPerNode;
    private final int relsPerNode;
    private final int maxRelsPerNode;
    private final int propsPerRel;
    private Report report;
    private BufferedReader nodesReader;
    private RowData nodesData;
    private BufferedReader relsReader;
    private RowData relsData;
    private String[] relTypes;
    private int[] nodePropIds;
    private int[] relPropIds;
    private Object[] relHeader;
    private int[] relTypeIds;
    private final int relTypesCount;
    private Object[] relRowData;

    public ParallelImporter(File graphDb, File nodesFile, File relationshipsFile,
                            long nodesToCreate, int propsPerNode, int relsPerNode, int maxRelsPerNode, int propsPerRel, String[] relTypes) {
        this.graphDb = graphDb;
        this.nodesFile = nodesFile;
        this.relationshipsFile = relationshipsFile;
        this.nodesToCreate = nodesToCreate;
        this.propsPerNode = propsPerNode;
        this.relsPerNode = relsPerNode;
        this.maxRelsPerNode = maxRelsPerNode;
        this.propsPerRel = propsPerRel;
        this.relTypes = relTypes;
        this.relTypesCount = relTypes.length;
        report = createReport();
    }

    protected StdOutReport createReport() {
        return new StdOutReport(10 * 1000 * 1000, 100);
    }

    @SuppressWarnings("unchecked")
    public static void main(String[] args) throws Exception {
        final Params params = new Params("data/dir nodes.csv relationships.csv #nodes #max-props-per-node #usual-rels-pernode #max-rels-per-node #max-props-per-rel rel,types",args);
        if (params.invalid()) {
            System.err.printf("Usage java -jar batchimport.jar %s%n",params);
            System.exit(1);
        }
        File graphDb = params.file("data/dir");
        File nodesFile = params.file("nodes.csv");
        File relationshipsFile = params.file("relationships.csv");

        if (graphDb.exists()) {
            FileUtils.deleteRecursively(graphDb);
        }
        final long nodesToCreate = params.longValue("#nodes");
        ParallelImporter importer = new ParallelImporter(graphDb,nodesFile,relationshipsFile,
                nodesToCreate, params.intValue("#max-props-per-node"),
                params.intValue("#usual-rels-pernode"),
                params.intValue("#max-rels-per-node"),
                params.intValue("#max-props-per-rel"),
                params.string("rel,types").split(","));
        importer.init();
        long time = System.currentTimeMillis();
        try {
            importer.run();
        } finally {
            importer.finish();
        }
        time = System.currentTimeMillis() - time;
        log.info(nodesToCreate + " took " + time + " ms");

        if (RUN_CHECK) ConsistencyCheckTool.main(new String[]{graphDb.getAbsolutePath()});
    }

    private void finish() {
        inserter.shutdown();
        inserter.report();
        report.finishImport("Nodes");
    }

    private void run() {
        inserter.run();
    }

    private void init() {
        inserter = new DisruptorBatchInserter(graphDb.getAbsolutePath(), config(), nodesToCreate, this);
        inserter.init();
        report.reset();
    }


    private static Map<String, String> config() {
        return stringMap("use_memory_mapped_buffers", "true",
        //"dump_configuration", "true",
        "cache_type", "none",
        "neostore.nodestore.db.mapped_memory", "50M",
        "neostore.propertystore.db.mapped_memory", "1G",
        "neostore.relationshipstore.db.mapped_memory", "500M"
);
    }
        @Override
        public NodeStruct newInstance() {
            return new NodeStruct(propsPerNode);
        }

        @Override
        public void init(BatchInserterImpl inserter) {
            try {
                initReader();
                initProperties(inserter);
                initRelTypes(inserter);

                NodeStruct.classInit(relsPerNode, propsPerRel);
            } catch(IOException ioe) {
                throw new RuntimeException("Error during initialization",ioe);
            }
        }

    private void initRelTypes(BatchInserterImpl inserter) {
        inserter.createAllRelTypeIndexes(asList(relTypes));
        relTypeIds = new int[relTypes.length];
        for (int i = 0; i < relTypesCount; i++) relTypeIds[i] = inserter.getRelTypeId(relTypes[i]);
    }

    private void initReader() throws IOException {
        nodesReader = new BufferedReader(new FileReader(nodesFile), MEGABYTE);
        nodesData = new RowData(nodesReader.readLine(), "\t", 0);

        relHeader = new Object[3];
        relsReader = new BufferedReader(new FileReader(relationshipsFile), MEGABYTE);
        relsData = new RowData(relsReader.readLine(), "\t", 3);
    }

    private void initProperties(BatchInserterImpl inserter) {
        final String[] nodesFields = nodesData.getFields();
        final String[] relFields = relsData.getFields();

        List<String> propertyNames = new ArrayList<String>(asList(nodesFields));
        propertyNames.addAll(asList(relFields));

        inserter.createAllPropertyIndexes(propertyNames);

        nodePropIds = new int[nodesFields.length];
        for (int i = 0; i < nodePropIds.length; i++) nodePropIds[i] = inserter.getPropertyKeyId(nodesFields[i]);

        relPropIds = new int[relFields.length];
        for (int i = 0; i < relPropIds.length; i++) relPropIds[i] = inserter.getPropertyKeyId(relFields[i]);
    }

    @Override
    public void fillStruct(long nodeId, NodeStruct nodeStruct) {
        try {
            String nodesLine = nodesReader.readLine();
            if (nodesLine == null) throw new IllegalStateException("Less Node rows than indicated at id " + nodeId);

            final Object[] rowData = nodesData.updateArray(nodesLine,(Object[])null);
            addProperties(nodeStruct, rowData, nodesData.getCount(), nodePropIds);

            addRelationships(nodeId, nodeStruct);

            report.dots();
        } catch (IOException ioe) {
            throw new RuntimeException("Error reading data for node " + nodeId, ioe);
        }
    }

    private void addRelationships(long nodeId, NodeStruct nodeStruct) throws IOException {
        while (true) {
            // todo real record-class for relationship-row data
            if (relRowData==null) {
                String line = relsReader.readLine();
                if (line==null) break; // reached end
                relRowData = relsData.updateArray(line, relHeader);
            }

            long from = Long.parseLong((String)relHeader[0]);
            long to = Long.parseLong((String)relHeader[1]);
            long min = Math.min(from, to);
            if (min < nodeId)
                throw new IllegalStateException(String.format("relationship-rows not pre-sorted found id %d less than node-id %d", min, nodeId));
            if (min > nodeId) break; // keep row data

            long target = Math.max(from, to);
            final boolean outgoing = from == min;
            final Relationship rel = nodeStruct.addRel(target, outgoing, type(relHeader[2]));

            addProperties(rel, relRowData, relsData.getCount(), relPropIds);
            relRowData = null;
        }
    }

    private int type(Object relType) {
        for (int i=0;i<relTypesCount;i++)
            if (relTypes[i].equals(relType)) return relTypeIds[i];
        throw new IllegalStateException("Unknown Relationship-Type "+relType);
    }

    private void addProperties(PropertyHolder propertyHolder, Object[] rowData, int count, final int[] propIds) {
        for (int i=count-1;i>=0;i--) {
            if (rowData[i]==null) continue;
            propertyHolder.addProperty(propIds[i], rowData[i]);
        }
    }

    @Override
    public int getRelsPerNode() {
        return relsPerNode;
    }

    @Override
    public int getMaxRelsPerNode() {
        return maxRelsPerNode;
    }
}