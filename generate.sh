source ./settings.sh

mvn clean test-compile exec:java -Dexec.mainClass=org.neo4j.batchimport.TestDataGenerator -Dexec.classpathScope=test -Dexec.args=sorted
