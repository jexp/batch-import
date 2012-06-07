Tab separated csv input files.

Example data for the files is a small social network
The row number corresponds to the node-id (node 0 is the reference node)

Property values not listed will not be set on the nodes or properties.

nodes.csv

name  age works_on
Michael 37  neo4j
Rana  6
Selma 4

format rels.csv

start	end	type	since
1 2 FATHER_OF	2004-01-01
1	3	FATHER_OF  2007-09-01
2 3 SISTER_OF 2008-05-03


ynagzet:batchimport mh$ rm -rf target/db
ynagzet:batchimport mh$ mvn clean compile assembly:single
[INFO] Scanning for projects...
[INFO] ------------------------------------------------------------------------
[INFO] Building Simple Batch Importer
[INFO]    task-segment: [clean, compile, assembly:single]
[INFO] ------------------------------------------------------------------------
...
[INFO] Building jar: /Users/mh/java/neo/batchimport/target/batch-import-jar-with-dependencies.jar
[INFO] ------------------------------------------------------------------------
[INFO] BUILD SUCCESSFUL
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