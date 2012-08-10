package org.neo4j.batchimport;

import org.neo4j.graphdb.RelationshipType;
import org.neo4j.kernel.impl.util.FileUtils;
import org.neo4j.unsafe.batchinsert.BatchInserter;
import org.neo4j.unsafe.batchinsert.BatchInserters;
import org.neo4j.unsafe.batchinsert.BatchInserterIndexProvider;
import org.neo4j.unsafe.batchinsert.BatchInserterIndex;
import org.neo4j.unsafe.batchinsert.LuceneBatchInserterIndexProvider;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileReader;
import java.io.FileWriter;
import java.io.IOException;
import java.util.*;

import org.neo4j.helpers.collection.MapUtil;

import static org.neo4j.helpers.collection.MapUtil.map;
import static org.neo4j.helpers.collection.MapUtil.stringMap;
import static org.neo4j.index.impl.lucene.LuceneIndexImplementation.EXACT_CONFIG;
import static org.neo4j.index.impl.lucene.LuceneIndexImplementation.FULLTEXT_CONFIG;

public class Importer {
    private static Report report;
    private BatchInserter db;
    private BatchInserterIndexProvider lucene;
    
    public Importer(File graphDb) {
    	Map<String, String> config = new HashMap<String, String>();
    	try {
	        if (new File("batch.properties").exists()) {
	        	System.out.println("Using Existing Configuration File");
	        } else {
		        System.out.println("Writing Configuration File to batch.properties");
				FileWriter fw = new FileWriter( "batch.properties" );
                fw.append( "use_memory_mapped_buffers=true\n"
                        + "neostore.nodestore.db.mapped_memory=100M\n"
                        + "neostore.relationshipstore.db.mapped_memory=1G\n"
                        + "neostore.propertystore.db.mapped_memory=250M\n"
                        + "neostore.propertystore.db.strings.mapped_memory=100M\n"
		                 + "neostore.propertystore.db.arrays.mapped_memory=0M\n"
		                 + "neostore.propertystore.db.index.keys.mapped_memory=15M\n"
		                 + "neostore.propertystore.db.index.mapped_memory=15M" );
		        fw.close();
	        }

        config = MapUtil.load( new File(
                "batch.properties" ) );

        } catch (Exception e) {
    		System.out.println(e.getMessage());
        }
                
        db = BatchInserters.inserter(graphDb.getAbsolutePath(), config);
        lucene = new LuceneBatchInserterIndexProvider(db);
        report = new Report(10 * 1000 * 1000, 100);
    }

    public static void main(String[] args) throws IOException {
        if (args.length < 3) {
            System.err.println("Usage java -jar batchimport.jar data/dir nodes.csv relationships.csv [node_index node-index-name fulltext|exact nodes_index.csv rel_index rel-index-name fulltext|exact rels_index.csv ....]");
        }
        File graphDb = new File(args[0]);
        File nodesFile = new File(args[1]);
        File relationshipsFile = new File(args[2]);
        File indexFile;
        String indexName;
        String indexType;
        
        if (graphDb.exists()) {
            FileUtils.deleteRecursively(graphDb);
        }
        Importer importBatch = new Importer(graphDb);
        try {
            if (nodesFile.exists()) importBatch.importNodes(nodesFile);
            if (relationshipsFile.exists()) importBatch.importRelationships(relationshipsFile);         
			for (int i = 3; i < args.length; i = i + 4) {
				indexName = args[i+1];
				indexType = args[i+2];
				indexFile = new File(args[i + 3]);
				if (args[i].equals("node_index")) {
					if (indexFile.exists()) importBatch.importNodeIndexes(indexFile, indexName, indexType);
				} else {
					if (indexFile.exists()) importBatch.importRelationshipIndexes(indexFile, indexName, indexType);
				}
			
			}
		} finally {
            importBatch.finish();
        }
    }

    private void finish() {
        lucene.shutdown();
        db.shutdown();
        report.finish();
    }

    static class Data {
        private Object[] data;
        private final int offset;
        private final String delim;
        private final String[] fields;
        private final String[] lineData;
        private final Type types[];
        private final int lineSize;
        private int dataSize;

        public Data(String header, String delim, int offset) {
            this.offset = offset;
            this.delim = delim;
            fields = header.split(delim);
            lineSize = fields.length;
            types = parseTypes(fields);
            lineData = new String[lineSize];
            createMapData(lineSize, offset);
        }

        private Object[] createMapData(int lineSize, int offset) {
            dataSize = lineSize - offset;
            data = new Object[dataSize*2];
            for (int i = 0; i < dataSize; i++) {
                data[i * 2] = fields[i + offset];
            }
            return data;
        }

        private Type[] parseTypes(String[] fields) {
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

        private int split(String line) {
            final StringTokenizer st = new StringTokenizer(line, delim,true);
            int count=0;
            for (int i = 0; i < lineSize; i++) {
                String value = st.nextToken();
                if (value.equals(delim)) {
                    lineData[i] = null;
                } else {
                    lineData[i] = value.trim().isEmpty() ? null : value;
                    if (i< lineSize -1) st.nextToken();
                }
                if (i >= offset && lineData[i]!=null) {
                    data[count++]=fields[i];
                    data[count++]=types[i-offset].convert(lineData[i]);
                }
            }
            return count;
        }

        public Map<String,Object> update(String line, Object... header) {
            int nonNullCount = split(line);
            if (header.length > 0) {
                System.arraycopy(lineData, 0, header, 0, header.length);
            }

            if (nonNullCount == dataSize*2) {
                return map(data);
            }
            Object[] newData=new Object[nonNullCount];
            System.arraycopy(data,0,newData,0,nonNullCount);
            return map(newData);
        }

    }

    static class Report {
        private final long batch;
        private final long dots;
        private long count;
        private long total = System.currentTimeMillis(), time, batchTime;

        public Report(long batch, int dots) {
            this.batch = batch;
            this.dots = batch / dots;
        }

        public void reset() {
            count = 0;
            batchTime = time = System.currentTimeMillis();
        }

        public void finish() {
            System.out.println("\nTotal import time: "+ (System.currentTimeMillis() - total) / 1000 + " seconds ");
        }

        public void dots() {
            if ((++count % dots) != 0) return;
            System.out.print(".");
            if ((count % batch) != 0) return;
            long now = System.currentTimeMillis();
            System.out.println(" "+ (now - batchTime) + " ms for "+batch);
            batchTime = now;
        }

        public void finishImport(String type) {
            System.out.println("\nImporting " + count + " " + type + " took " + (System.currentTimeMillis() - time) / 1000 + " seconds ");
        }
    }

    private void importNodes(File file) throws IOException {
        BufferedReader bf = new BufferedReader(new FileReader(file));
        final Data data = new Data(bf.readLine(), "\t", 0);
        String line;
        report.reset();
        while ((line = bf.readLine()) != null) {
            db.createNode(data.update(line));
            report.dots();
        }
        report.finishImport("Nodes");
    }

    private void importRelationships(File file) throws IOException {
        BufferedReader bf = new BufferedReader(new FileReader(file));
        final Data data = new Data(bf.readLine(), "\t", 3);
        Object[] rel = new Object[3];
        final RelType relType = new RelType();
        String line;
        report.reset();
        while ((line = bf.readLine()) != null) {
            final Map<String, Object> properties = data.update(line, rel);
            db.createRelationship(id(rel[0]), id(rel[1]), relType.update(rel[2]), properties);
            report.dots();
        }
        report.finishImport("Relationships");
    }

    private void importNodeIndexes(File file, String indexName, String indexType) throws IOException {
    	BatchInserterIndex index;
    	if (indexType.equals("fulltext")) {
    		index = lucene.nodeIndex( indexName, FULLTEXT_CONFIG );
    	} else {
    		index = lucene.nodeIndex( indexName, EXACT_CONFIG );
    	}
        
        BufferedReader bf = new BufferedReader(new FileReader(file));
        
        final Data data = new Data(bf.readLine(), "\t", 1);
        Object[] node = new Object[1];
        String line;
        report.reset();
        while ((line = bf.readLine()) != null) {        
            final Map<String, Object> properties = data.update(line, node);
            index.add(id(node[0]), properties);
            report.dots();
        }
                
        report.finishImport("Nodes into " + indexName + " Index");
    }

    private void importRelationshipIndexes(File file, String indexName, String indexType) throws IOException {
    	BatchInserterIndex index;
    	if (indexType.equals("fulltext")) {
    		index = lucene.relationshipIndex( indexName, FULLTEXT_CONFIG );
    	} else {
    		index = lucene.relationshipIndex( indexName, EXACT_CONFIG );
    	}

        BufferedReader bf = new BufferedReader(new FileReader(file));
        
        final Data data = new Data(bf.readLine(), "\t", 1);
        Object[] rel = new Object[1];
        String line;
        report.reset();
        while ((line = bf.readLine()) != null) {        
            final Map<String, Object> properties = data.update(line, rel);
            index.add(id(rel[0]), properties);
            report.dots();
        }
                
        report.finishImport("Relationships into " + indexName + " Index");

    }


    static class RelType implements RelationshipType {
        String name;

        public RelType update(Object value) {
            this.name = value.toString();
            return this;
        }

        public String name() {
            return name;
        }
    }

    enum Type {
        BOOLEAN {
            @Override
            public Object convert(String value) {
                return Boolean.valueOf(value);
            }
        },
        INT {
            @Override
            public Object convert(String value) {
                return Integer.valueOf(value);
            }
        },
        LONG {
            @Override
            public Object convert(String value) {
                return Long.valueOf(value);
            }
        },
        DOUBLE {
            @Override
            public Object convert(String value) {
                return Double.valueOf(value);
            }
        },
        FLOAT {
            @Override
            public Object convert(String value) {
                return Float.valueOf(value);
            }
        },
        BYTE {
            @Override
            public Object convert(String value) {
                return Byte.valueOf(value);
            }
        },
        SHORT {
            @Override
            public Object convert(String value) {
                return Short.valueOf(value);
            }
        },
        CHAR {
            @Override
            public Object convert(String value) {
                return value.charAt(0);
            }
        },
        STRING {
            @Override
            public Object convert(String value) {
                return value;
            }
        };

        private static Type fromString(String typeString) {
            if (typeString==null || typeString.isEmpty()) return Type.STRING;
            try {
                return valueOf(typeString.toUpperCase());
            } catch (Exception e) {
                throw new IllegalArgumentException("Unknown Type "+typeString);
            }
        }

        public abstract Object convert(String value);
    }

    private long id(Object id) {
        return Long.parseLong(id.toString());
    }
}