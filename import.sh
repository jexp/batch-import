if [ ! -d lib ]; then 
  echo lib directory of binary download missing. Please download the zip or run import-mvn.sh
  exit 1
fi

HEAP=4G

# Detect Cygwin
case `uname -s` in
CYGWIN*)
    cygwin=1
esac

DB=${1-target/graph.db}
shift
NODES=${1-nodes.csv}
shift
RELS=${1-rels.csv}
shift
CP=""
base=`dirname "$0"`
if [ \! -z "$cygwin" ]; then
    wbase=`cygpath -w "$base"`
fi
curdir=`pwd`
cd "$base"
for i in lib/*.jar; do
    if [ -z "$cygwin" ]; then
        CP="$CP":"$base/$i"
    else
        i=`cygpath -w "$i"`
        CP="$CP;$wbase/$i"
    fi
done
cd "$curdir"
#echo java -classpath $CP -Xmx$HEAP -Xms$HEAP -Dfile.encoding=UTF-8 org.neo4j.batchimport.Importer batch.properties "$DB" "$NODES" "$RELS" "$@"
java -classpath "$CP" -Xmx$HEAP -Xms$HEAP -Dfile.encoding=UTF-8 org.neo4j.batchimport.Importer batch.properties "$DB" "$NODES" "$RELS" "$@"
