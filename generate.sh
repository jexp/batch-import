source ./settings.sh

mvn clean test-compile exec:java -Dexec.mainClass=org.neo4j.batchimport.TestDataGenerator -Dexec.classpathScope=test \
-Dexec.args="$1 $2 $3 $4"  | grep -iv '\[\(INFO\|debug\)\]'
