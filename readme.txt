Tab separated csv input files.

Example data for the files is a small social network

Property names in first row.
Property values not listed will not be set on the nodes or properties.

The row number corresponds to the node-id (node 0 is the reference node)

nodes.csv

name    age works_on
Michael 37  neo4j
Rana    6
Selma   4

rels.csv

start	end	type	    since
1     2   FATHER_OF	2004-01-01
1     3   FATHER_OF 2007-09-01
2     3   SISTER_OF 2008-05-03


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

Optionally you can add nodes and relationships to indexes.

Add four arguments per each index to command line:

To create a full text node index called users using nodes_index.csv:
node_index users fulltext nodes_index.csv 

To create an exact relationship index called worked using rels_index.csv:
rel_index worked exact rels_index.csv

Example command line:
java -server -Xmx4G -jar ../batch-import/target/batch-import-jar-with-dependencies.jar neo4j/data/graph.db nodes.csv rels.csv node_index users fulltext nodes_index.csv rel_index worked exact rels_index.csv

nodes_index.csv

id	name	language
1	Victor Richards	West Frisian
2	Virginia Shaw	Korean
3	Lois Simpson	Belarusian
4	Randy Bishop	Hiri Motu
5	Lori Mendoza	Tok Pisin

rels_index.csv

id	property1	property2
0	cwqbnxrv	rpyqdwhk
1	qthnrret	tzjmmhta
2	dtztaqpy	pbmcdqyc
