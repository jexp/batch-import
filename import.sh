DB=${1-target/graph.db}
shift
NODES=${1-nodes.csv}
shift
RELS=${1-rels.csv}
shift
mvn compile exec:java -Dexec.mainClass="org.neo4j.batchimport.Importer" \
   -Dexec.args="batch.properties $DB $NODES $RELS $@" | grep -iv '\[\(INFO\|debug\)\]'