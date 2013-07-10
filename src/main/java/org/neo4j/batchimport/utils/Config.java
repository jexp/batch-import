package org.neo4j.batchimport.utils;

import org.neo4j.batchimport.Importer;
import org.neo4j.batchimport.IndexInfo;
import org.neo4j.helpers.collection.MapUtil;

import java.io.File;
import java.io.FileWriter;
import java.util.*;

import static org.neo4j.batchimport.utils.Properties.*;

public class Config {
    public static final String CONFIG_FILE_NAME = "batch.properties";

    private final Map<String, String> configData;

    public Config(Map<String, String> configData) {
        this.configData = configData;
    }

    public static Config convertArgumentsToConfig(String[] args) {
        final Stack<String> argumentList = toStack(args);

        String configFileName = findConfigFileName(argumentList);

        final Map<String, String> config = config(configFileName);

        convertParamsToConfig(argumentList, config);

        validateConfig(config);
        return new Config(config);
    }

    private static Stack<String> toStack(String[] args) {
        final Stack<String> argumentList = new Stack<String>();
        for (int i = args.length - 1; i >= 0; i--) {
            argumentList.push(args[i]);
        }
        return argumentList;
    }

    private static String findConfigFileName(Stack<String> argumentList) {
        String firstParam = argumentList.isEmpty() ? "" : argumentList.peek();
        String configFileName = CONFIG_FILE_NAME;
        if (firstParam.endsWith(".properties")) {
            configFileName = firstParam;
            popOrNull(argumentList);
        }
        return configFileName;
    }

    // todo more checks ?
    private static void validateConfig(Map<String, String> config) {
        if (!config.containsKey(BATCH_IMPORT_GRAPH_DB.key())) throw new IllegalArgumentException("Missing parameter for graphdb directory");
    }

    private static Collection<IndexInfo> convertParamsToConfig(Stack<String> args, Map<String, String> config) {
        addConfigParamIfArgument(args, config, BATCH_IMPORT_GRAPH_DB.key());
        addConfigParamIfArgument(args, config, BATCH_IMPORT_NODES_FILES.key());
        addConfigParamIfArgument(args, config, BATCH_IMPORT_RELS_FILES.key());
        Collection<IndexInfo> indexes = createIndexInfos(args);
        for (IndexInfo index : indexes) {
            index.addToConfig(config);
        }
        return indexes;
    }

    private static void addConfigParamIfArgument(Stack<String> args, Map<String, String> config, String param) {
        final String arg = popOrNull(args);
        if (arg==null || arg.trim().isEmpty()) return;
        if (!config.containsKey(param)) config.put(param, arg);
    }

    private static String popOrNull(Stack<String> args) {
        if (args.isEmpty()) return null;
        return args.pop();
    }

    private static Collection<IndexInfo> createIndexInfos(Stack<String> args) {
        Collection<IndexInfo> indexes=new ArrayList<IndexInfo>();
        while (!args.isEmpty()) {
            indexes.add(new IndexInfo(popOrNull(args), popOrNull(args), popOrNull(args), popOrNull(args)));
        }
        return indexes;
    }

    public static Map<String, String> config(String fileName) {
        Map<String, String> config = new HashMap<String, String>();
        try {
            if (new File(fileName).exists()) {
                System.out.println("Using Existing Configuration File");
            } else {
                System.out.println("Writing Configuration File to batch.properties");
                FileWriter fw = new FileWriter(fileName);
                fw.append("use_memory_mapped_buffers=true\n"
                        + "neostore.nodestore.db.mapped_memory=100M\n"
                        + "neostore.relationshipstore.db.mapped_memory=500M\n"
                        + "neostore.propertystore.db.mapped_memory=1G\n"
                        + "neostore.propertystore.db.strings.mapped_memory=200M\n"
                        + "neostore.propertystore.db.arrays.mapped_memory=0M\n"
                        + "neostore.propertystore.db.index.keys.mapped_memory=15M\n"
                        + "neostore.propertystore.db.index.mapped_memory=15M");
                fw.close();
            }

            config = MapUtil.load(new File(fileName));

        } catch (Exception e) {
            System.out.println(e.getMessage());
        }
        return config;
    }

    public static Collection<IndexInfo> extractIndexInfos(Map<String, String> config) {
        Collection<IndexInfo>  result=new ArrayList<IndexInfo>();
        for (Map.Entry<String, String> entry : config.entrySet()) {
            final IndexInfo info = IndexInfo.fromConfigEntry(entry);
            if (info!=null) result.add(info);
        }
        return result;
    }

    public static boolean configOptionEnabled(Config config, String option) {
        return "true".equalsIgnoreCase(config.get(option));
    }

    public static Collection<File> toFiles(String commaSeparatedFileList) {
        Collection<File> files=new ArrayList<File>();
        for (String part : commaSeparatedFileList.split(",")) {
            final File file = new File(part);
            if (file.exists() && file.canRead() && file.isFile()) files.add(file);
            else System.err.println("File "+file+" does not exist, can not be read or is not a file.");
        }
        return files;
    }

    public static String NODE_INDEX(String indexName) {
        return "batch_import.node_index." + indexName;
    }
    public static String RELATIONSHIP_INDEX(String indexName) {
        return "batch_import.relationship_index." + indexName;
    }

    public boolean isCachedIndexDisabled() {
        return configOptionEnabled(this, BATCH_IMPORT_MAPDB_CACHE_DISABLE.key());
    }

    public Collection<IndexInfo> getIndexInfos() {
        return extractIndexInfos(configData);
    }

    public Collection<File> getRelsFiles() {
        return toFiles(get(BATCH_IMPORT_RELS_FILES.key()));
    }

    public Collection<File> getNodesFiles() {
        return toFiles(get(BATCH_IMPORT_NODES_FILES.key()));
    }

    public char getDelimChar(Importer importer) {
        final String delim = get(BATCH_IMPORT_CSV_DELIM.key());
        if (delim==null || delim.isEmpty()) return '\t';
        return delim.trim().charAt(0);
    }

    public boolean quotesEnabled() {
        return configOptionEnabled(this, BATCH_IMPORT_CSV_QUOTES.key());
    }

    public String getGraphDbDirectory() {
        return get(BATCH_IMPORT_GRAPH_DB.key());
    }

    String get(String option) {
        return configData.get(option);
    }

    public boolean keepDatabase() {
        return configOptionEnabled(this, BATCH_IMPORT_KEEP_DB.key());
    }

    public Map<String, String> getConfigData() {
        return configData;
    }
}
