package org.neo4j.batchimport;

import org.neo4j.graphdb.RelationshipType;
import org.neo4j.unsafe.batchinsert.BatchInserter;
import org.neo4j.unsafe.batchinsert.BatchInserters;
import org.neo4j.unsafe.batchinsert.BatchInserterImpl;
import org.neo4j.unsafe.batchinsert.BatchInserterIndexProvider;
import org.neo4j.unsafe.batchinsert.BatchInserterIndex;
import org.neo4j.unsafe.batchinsert.LuceneBatchInserterIndexProvider;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileReader;
import java.io.FileWriter;
import java.io.IOException;
import java.util.HashMap;
import java.util.Map;
import org.neo4j.helpers.collection.MapUtil;

import static org.neo4j.helpers.collection.MapUtil.map;
import static org.neo4j.helpers.collection.MapUtil.stringMap;
import static org.neo4j.index.impl.lucene.LuceneIndexImplementation.EXACT_CONFIG;

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
		        fw.append( "neostore.nodestore.db.mapped_memory=100M\n"
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
            System.err.println("Usage java -jar batchimport.jar data/dir nodes.csv relationships.csv");
        }
        File graphDb = new File(args[0]);
        File nodesFile = new File(args[1]);
        File relationshipsFile = new File(args[2]);
        File indexFile = new File("");
        String indexName = "";
        String indexType = "";
        
        if (!graphDb.exists()) graphDb.mkdirs();
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
        private final Object[] data;
        private final int offset;
        private final String delim;

        public Data(String header, String delim, int offset) {
            this.offset = offset;
            this.delim = delim;
            String[] fields = header.split(delim);
            data = new Object[(fields.length - offset) * 2];
            for (int i = 0; i < fields.length - offset; i++) {
                data[i * 2] = fields[i + offset];
            }
        }

        public Object[] update(String line, Object... header) {
            final String[] values = line.split(delim);
            if (header.length > 0) {
                System.arraycopy(values, 0, header, 0, header.length);
            }
            for (int i = 0; i < values.length - offset; i++) {
                data[i * 2 + 1] = values[i + offset];
            }
            return data;
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
            System.out.println((System.currentTimeMillis() - total) / 1000 + " seconds ");
        }

        public void dots() {
            if ((++count % dots) != 0) return;
            System.out.print(".");
            if ((count % batch) != 0) return;
            long now = System.currentTimeMillis();
            System.out.println((now - batchTime) + " ms for "+batch);
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
            db.createNode(map(data.update(line)));
            report.dots();
        }
        report.finishImport("Nodes");
    }

    private void importRelationships(File file) throws IOException {
        BufferedReader bf = new BufferedReader(new FileReader(file));
        final Data data = new Data(bf.readLine(), "\t", 3);
        Object[] rel = new Object[3];
        final Type type = new Type();
        String line;
        report.reset();
        while ((line = bf.readLine()) != null) {
            final Map<String, Object> properties = map(data.update(line, rel));
            db.createRelationship(id(rel[0]), id(rel[1]), type.update(rel[2]), properties);
            report.dots();
        }
        report.finishImport("Relationships");
    }

    private void importNodeIndexes(File file, String indexName, String indexType) throws IOException {
    	BatchInserterIndex index;
    	if (indexType.equals("fulltext")) {
    		index = lucene.nodeIndex( indexName, stringMap( "type", "fulltext" ) );
    	} else {
    		index = lucene.nodeIndex( indexName, EXACT_CONFIG );
    	}
        
        BufferedReader bf = new BufferedReader(new FileReader(file));
        
        final Data data = new Data(bf.readLine(), "\t", 1);
        Object[] node = new Object[1];
        String line;
        report.reset();
        while ((line = bf.readLine()) != null) {        
            final Map<String, Object> properties = map(data.update(line, node));
            index.add(id(node[0]), properties);
            report.dots();
        }
                
        report.finishImport("Nodes into " + indexName + " Index");
    }

    private void importRelationshipIndexes(File file, String indexName, String indexType) throws IOException {
    	BatchInserterIndex index;
    	if (indexType.equals("fulltext")) {
    		index = lucene.relationshipIndex( indexName, stringMap( "type", "fulltext" ) );
    	} else {
    		index = lucene.relationshipIndex( indexName, EXACT_CONFIG );
    	}

        BufferedReader bf = new BufferedReader(new FileReader(file));
        
        final Data data = new Data(bf.readLine(), "\t", 1);
        Object[] rel = new Object[1];
        String line;
        report.reset();
        while ((line = bf.readLine()) != null) {        
            final Map<String, Object> properties = map(data.update(line, rel));
            index.add(id(rel[0]), properties);
            report.dots();
        }
                
        report.finishImport("Relationships into " + indexName + " Index");

    }


    static class Type implements RelationshipType {
        String name;

        public Type update(Object value) {
            this.name = value.toString();
            return this;
        }

        public String name() {
            return name;
        }
    }

    private long id(Object id) {
        return Long.parseLong(id.toString());
    }
}