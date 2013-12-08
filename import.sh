MEMORY=4G
DB=${1-target/graph.db}
shift
NODES=${1-nodes.csv}
shift
RELS=${1-rels.csv}
shift
CP=""
for i in lib/*.jar; do CP="$CP":"$i"; done
java -classpath $CP -Xmx$MEMORY -Xms$MEMORY org.neo4j.batchimport.Importer batch.properties "$DB" "$NODES" "$RELS" "$@"
