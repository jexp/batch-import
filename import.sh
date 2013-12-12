HEAP=4G
DB=${1-target/graph.db}
shift
NODES=${1-nodes.csv}
shift
RELS=${1-rels.csv}
shift
CP=""
base=`dirname "$0"`
curdir=`pwd`
cd "$base"
for i in lib/*.jar; do CP="$CP":"$base/$i"; done
cd "$curdir"
#echo java -classpath $CP -Xmx$HEAP -Xms$HEAP -Dfile.encoding=UTF-8 org.neo4j.batchimport.Importer batch.properties "$DB" "$NODES" "$RELS" "$@"
java -classpath "$CP" -Xmx$HEAP -Xms$HEAP -Dfile.encoding=UTF-8 org.neo4j.batchimport.Importer batch.properties "$DB" "$NODES" "$RELS" "$@"
