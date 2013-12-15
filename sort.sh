HEAP=4G
IN=${1-rels.csv}
shift
OUT=${1-rels-sorted.csv}
CP=""
for i in lib/*.jar; do CP="$CP":"$i"; done

echo java -classpath $CP -Xmx$HEAP -Xms$HEAP -Dfile.encoding=UTF-8 org.neo4j.batchimport.utils.RelationshipSorter "$IN" "$OUT"
java -classpath $CP -Xmx$HEAP -Xms$HEAP -Dfile.encoding=UTF-8 org.neo4j.batchimport.utils.RelationshipSorter "$IN" "$OUT"
