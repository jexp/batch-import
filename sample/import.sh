echo "Run in main directory sh sample/import.sh"
mvn test-compile exec:java -Dexec.mainClass="org.neo4j.batchimport.Importer" \
  -Dexec.args="sample/batch.properties target/graph.db sample/nodes.csv,sample/nodes2.csv sample/rels.csv"