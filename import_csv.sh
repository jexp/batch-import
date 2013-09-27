source ./settings.sh

mvn clean test-compile exec:java -Dexec.mainClass=org.neo4j.batchimport.ParallelImporter -Dexec.classpathScope=test -Dexec.args="/mnt/parallel.db nodes.csv rels.csv 100000000 4 50 100 2 ONE,TWO,THREE,FOUR,FIVE,SIX,SEVEN,EIGHT,NINE,TEN"
