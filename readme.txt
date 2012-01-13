Tab separated input files.
format nodes.csv

propname1	propname2
value1	value2

format rels.csv

start	end	type	propname1	propname2
0	1	OWNS	value1	value2
1	2	IS_A	value1	value2


ynagzet:batchimport mh$ rm -rf target/db
ynagzet:batchimport mh$ mvn clean compile assembly:single
[INFO] Scanning for projects...
[INFO] ------------------------------------------------------------------------
[INFO] Building Simple Batch Importer
[INFO]    task-segment: [clean, compile, assembly:single]
[INFO] ------------------------------------------------------------------------
[INFO] [clean:clean {execution: default-clean}]
[INFO] Deleting directory /Users/mh/java/neo/batchimport/target
[INFO] [resources:resources {execution: default-resources}]
[WARNING] Using platform encoding (MacRoman actually) to copy filtered resources, i.e. build is platform dependent!
[INFO] skip non existing resourceDirectory /Users/mh/java/neo/batchimport/src/main/resources
[INFO] [compiler:compile {execution: default-compile}]
[WARNING] File encoding has not been set, using platform encoding MacRoman, i.e. build is platform dependent!
[INFO] Compiling 1 source file to /Users/mh/java/neo/batchimport/target/classes
[INFO] [assembly:single {execution: default-cli}]
[INFO] Processing DependencySet (output=)
[INFO] Building jar: /Users/mh/java/neo/batchimport/target/batch-import-jar-with-dependencies.jar
[INFO] ------------------------------------------------------------------------
[INFO] BUILD SUCCESSFUL
[INFO] ------------------------------------------------------------------------
[INFO] Total time: 2 seconds
[INFO] Finished at: Fri Jan 13 04:06:06 CET 2012
[INFO] Final Memory: 17M/81M
[INFO] ------------------------------------------------------------------------
ynagzet:batchimport mh$ java -server -Xmx4G -jar target/batch-import-jar-with-dependencies.jar target/db nodes.csv rels.csv 
Physical mem: 16384MB, Heap size: 3640MB
use_memory_mapped_buffers=false
neostore.propertystore.db.index.keys.mapped_memory=5M
neostore.propertystore.db.strings.mapped_memory=100M
neostore.propertystore.db.arrays.mapped_memory=215M
neo_store=/Users/mh/java/neo/batchimport/target/db/neostore
neostore.relationshipstore.db.mapped_memory=1000M
neostore.propertystore.db.index.mapped_memory=5M
neostore.propertystore.db.mapped_memory=1000M
dump_configuration=true
cache_type=none
neostore.nodestore.db.mapped_memory=200M
...........................................................................
Importing 7500000 Nodes took 17 seconds 
....................................................................................................35818 ms
....................................................................................................39343 ms
....................................................................................................41788 ms
....................................................................................................48897 ms
............
Importing 41246740 Relationships took 170 seconds 
212 seconds 
ynagzet:batchimport mh$ du -sh target/db/
3,2G	target/db/