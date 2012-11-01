MEMORY_OPTS="-Xmx50G -Xms50G -server -d64 -Xmn3g -XX:SurvivorRatio=2"
GC_OPTS=""

PRINT_GC_OPTS=" -XX:+PrintGCApplicationStoppedTime  -XX:+PrintTenuringDistribution -XX:+PrintGCDetails -XX:+PrintGCDateStamps -Xloggc:gc.log"

#-XX:+PrintGCApplicationConcurrentTime -XX:+PrintHeapAtGC -XX:+PrintGCTaskTimeStamps

MAVEN_OPTS="$MEMORY_OPTS $GC_OPTS $PRINT_GC_OPTS" mvn clean compile exec:java -Dexec.mainClass=org.neo4j.batchimport.DisruptorTest -Dexec.classpathScope=test