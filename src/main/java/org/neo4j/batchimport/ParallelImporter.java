package org.neo4j.batchimport;

import org.apache.log4j.Logger;
import org.neo4j.batchimport.importer.Type;
import org.neo4j.batchimport.structs.NodeStruct;
import org.neo4j.batchimport.structs.PropertyHolder;
import org.neo4j.batchimport.structs.Relationship;
import org.neo4j.batchimport.utils.Chunker;
import org.neo4j.batchimport.utils.Params;
import org.neo4j.consistency.ConsistencyCheckTool;
import org.neo4j.helpers.collection.MapUtil;
import org.neo4j.kernel.impl.util.FileUtils;
import org.neo4j.unsafe.batchinsert.BatchInserterImpl;

import java.io.*;
import java.net.URL;
import java.util.ArrayList;
import java.util.Arrays;
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

public class ParallelImporter implements NodeStructFactory {

    private static final int MEGABYTE = 1024 * 1024;

    private final static Logger log = Logger.getLogger(ParallelImporter.class);

    private static final File PROP_FILE = new File("batch.properties");
    private DisruptorBatchInserter inserter;
    private final File graphDb;

    // config options
    private final boolean runCheck;
    private final long nodesToCreate;
    private final int propsPerNode;
    private final int relsPerNode;
    private final int maxRelsPerNode;
    private final int propsPerRel;
    private final char delim;

    private final String nodesFile;
    private BufferedReader nodesReader;
    private Chunker nodeChunker;
    private final String relationshipsFile;
    private BufferedReader relsReader;
    private Chunker relChunker;

    private String[] relTypes;
    private int[] nodePropIds;
    private int nodePropCount;
    private int[] relPropIds;
    private int relPropCount;
    private int[] relTypeIds;
    private final int relTypesCount;

    private Report report;

    private long from = -1;
    private long to = -1;
    private Type[] nodePropertyTypes;
    private Type[] relPropertyTypes;

    public ParallelImporter(File graphDb, String nodesFile, String relationshipsFile,
                            long nodesToCreate, int propsPerNode, int relsPerNode, int maxRelsPerNode, int propsPerRel, String[] relTypes, final char delim, final boolean runCheck) {
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
        this.delim = delim;
        this.runCheck = runCheck;
        report = createReport();
    }

    protected StdOutReport createReport() {
        return new StdOutReport(10 * 1000 * 1000, 100);
    }

    @SuppressWarnings("unchecked")
    public static void main(String... args) throws Exception {
        final Params params = new Params("data/dir nodes.csv relationships.csv #nodes #max-props-per-node #usual-rels-pernode #max-rels-per-node #max-props-per-rel rel,types",args);
        if (params.invalid()) {
            System.err.printf("Usage java -jar batchimport.jar %s%n",params);
            return;
        }
        File graphDb = params.file("data/dir");
        String nodesFile = params.string("nodes.csv");
        String relationshipsFile = params.string("relationships.csv");

        if (graphDb.exists()) {
            FileUtils.deleteRecursively(graphDb);
        }
        final long nodesToCreate = params.longValue("#nodes");
        ParallelImporter importer = new ParallelImporter(graphDb,nodesFile,relationshipsFile,
                nodesToCreate, params.intValue("#max-props-per-node"),
                params.intValue("#usual-rels-pernode"),
                params.intValue("#max-rels-per-node"),
                params.intValue("#max-props-per-rel"),
                params.string("rel,types").split(","), '\t', false);
        importer.init();
        long time = System.currentTimeMillis();
        try {
            importer.run();
        } finally {
            importer.finish();
        }
        time = System.currentTimeMillis() - time;
        log.info(nodesToCreate + " took " + time + " ms");

        if (importer.runCheck) ConsistencyCheckTool.main(new String[]{graphDb.getAbsolutePath()});
    }

    private void finish() {
        inserter.shutdown();
        inserter.report();
        report.finishImport("");
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
        nodesReader = new BufferedReader(readerFor(nodesFile), MEGABYTE);
        nodeChunker = new Chunker(nodesReader, delim);

        relsReader = new BufferedReader(readerFor(relationshipsFile), MEGABYTE);
        relChunker = new Chunker(relsReader, delim);
    }

    private Reader readerFor(String file) throws IOException {
        if (file.startsWith("http")) return new InputStreamReader(new URL(file).openStream());
        if (new File(file).exists()) return new FileReader(file);
        throw new IOException("Input File "+file+" does not exist");
    }

    
    private Type[] parseTypes(String[] fields) {
        int lineSize = fields.length;
        Type[] types = new Type[lineSize];
        Arrays.fill(types, Type.STRING);
        for (int i = 0; i < lineSize; i++) {
            String field = fields[i];
            int idx = field.indexOf(':');
            if (idx!=-1) {
               fields[i]=field.substring(0,idx);
               types[i]= Type.fromString(field.substring(idx + 1));
            }
        }
        return types;
    }

    private void initProperties(BatchInserterImpl inserter) throws IOException {

        final String[] nodesFields = nodesReader.readLine().split(String.valueOf(delim));
        nodePropertyTypes = parseTypes(nodesFields);
        nodePropCount = nodesFields.length;
        String[] relFields = relsReader.readLine().split(String.valueOf(delim));
        relFields = Arrays.copyOfRange(relFields, 3, relFields.length);
        relPropertyTypes = parseTypes(relFields);
        relPropCount = relFields.length;
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

            if (nodeId>=nodesToCreate) throw new IllegalStateException("Already at "+nodeId+" but only configured to import "+nodesToCreate+" nodes");
            addProperties(nodeStruct,nodeChunker, nodePropIds,nodePropCount,nodePropertyTypes);

            addRelationships(nodeId, nodeStruct);

            report.dots();
        } catch (IOException ioe) {
            throw new RuntimeException("Error reading data for node " + nodeId, ioe);
        }
    }

    private void addRelationships(long nodeId, NodeStruct nodeStruct) throws IOException {
        while (true) {
            if (from == -1) {
                final String token = relChunker.nextWord();
                if (token==Chunker.EOF) return;
                from = Long.parseLong(token);
            }
            if (to == -1) to = Long.parseLong(relChunker.nextWord());
            long min = Math.min(from, to);
            if (min < nodeId)
                throw new IllegalStateException(String.format("relationship-rows not pre-sorted found id %d less than node-id %d", min, nodeId));
            if (min > nodeId) break; // keep already parsed data

            long target = Math.max(from, to);
            final boolean outgoing = from == min;
            final Relationship rel = nodeStruct.addRel(target, outgoing, type(relChunker.nextWord()));

            addProperties(rel, relChunker, relPropIds,relPropCount, relPropertyTypes);
            from = -1;
            to = -1;
        }
    }

    private int type(Object relType) {
        for (int i=0;i<relTypesCount;i++)
            if (relTypes[i].equals(relType)) return relTypeIds[i];
        throw new IllegalStateException("Unknown Relationship-Type "+relType);
    }

    private void addProperties(PropertyHolder propertyHolder, Chunker nodeChunker, final int[] propIds, int count, Type[] propertyTypes) throws IOException {
        String value;
        int i=0;
        do {
            value = nodeChunker.nextWord();
            if (Chunker.NO_VALUE != value && Chunker.EOL != value && Chunker.EOF != value && i<count) {
                Object converted = propertyTypes[i] == Type.STRING ? value : propertyTypes[i].convert(value);
                propertyHolder.addProperty(propIds[i], converted);
            }
            i++;
        } while (value!=Chunker.EOF && value!=Chunker.EOL);
    }

    @Override
    public int getRelsPerNode() {
        return relsPerNode;
    }

    @Override
    public int getMaxRelsPerNode() {
        return maxRelsPerNode;
    }

    public long getTotalNrOfRels() {
        return getRelsPerNode() * nodesToCreate;
    }
}