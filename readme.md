# Neo4j (CSV) Batch Importer

You provide one tab separated csv file for nodes and one for relationships (optionally more for indexes)

Example data for the files is a small social network

## File format

* Property names in first row.
* The row number corresponds to the node-id (node 0 is the reference node)
* Property values not listed will not be set on the nodes or properties.
* Optionally property fields can have a type (defaults to String) indicated with name:type where type is one of (int, long, float, double, boolean, byte, short, char, string). The string value is then converted to that type. Conversion failure will result in abort of the import operation.

## Examples

### nodes.csv

    name    age works_on
    Michael 37  neo4j
    Selina  14
    Rana    6
    Selma   4

### rels.csv

    start	end	type	    since   counter:int
    1     2   FATHER_OF	1998-07-10  1
    1     3   FATHER_OF 2007-09-15  2
    1     4   FATHER_OF 2008-05-03  3
    3     4   SISTER_OF 2008-05-03  5
    2     3   SISTER_OF 2007-09-15  7


## Execution

    java -server -Xmx4G -jar ../batch-import/target/batch-import-jar-with-dependencies.jar neo4j/data/graph.db nodes.csv rels.csv


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


## Indexing

Optionally you can add nodes and relationships to indexes.

Add four arguments per each index to command line:

To create a full text node index called users using nodes_index.csv:

    node_index users fulltext nodes_index.csv

To create an exact relationship index called worked using rels_index.csv:

    rel_index worked exact rels_index.csv

Example command line:

    java -server -Xmx4G -jar ../batch-import/target/batch-import-jar-with-dependencies.jar neo4j/data/graph.db nodes.csv rels.csv node_index users fulltext nodes_index.csv rel_index worked exact rels_index.csv

## Examples

### nodes_index.csv

    id	name	language
    1	Victor Richards	West Frisian
    2	Virginia Shaw	Korean
    3	Lois Simpson	Belarusian
    4	Randy Bishop	Hiri Motu
    5	Lori Mendoza	Tok Pisin

### rels_index.csv

    id	property1	property2
    0	cwqbnxrv	rpyqdwhk
    1	qthnrret	tzjmmhta
    2	dtztaqpy	pbmcdqyc



# Parallel Batch inserter with Neo4j

Uses the [LMAX Disruptor](http://lmax-exchange.github.com/disruptor/) to parallelize operations during batch-insertion.

## The 6 operations are:

1. property encoding
2. property-record creation
3. relationship-id creation and forward handling of reverse relationship chains
4. writing node-records
5. writing relationship-records
6. writing property-records

## Dependencies:

    (1)<--(2)<--(6)
    (2)<--(5)-->(3)   
    (2)<--(4)-->(3)   

It uses the above dependency setup of disruptor handlers to execute the different concerns in parallel. A ringbuffer of about 2^18 elements is used and a heap size of 5-20G, MMIO configuration within the heap limits.

## Execution:

   MAVEN_OPTS="-Xmx5G -Xms5G -server -d64 -XX:NewRatio=5"  mvn clean test-compile exec:java -Dexec.mainClass=org.neo4j.batchimport.DisruptorTest -Dexec.classpathScope=test

## current limitations, constraints:

* only up to 2bn relationships (due to an int based multi-map)
* have to know max # of rels per node, properties per node and relationship
* relationships have to be pre-sorted by min(start,end)

## measurements

We successfully imported 2bn nodes (2 properties) and 20bn relationships (1 property) in 11 hours on an EC2 high-IO instance,
with 35 ECU, 60GB RAM, 2TB SSD writing up to 200MB/s, resulting in a store of 1.4 TB. That makes around 500k elements per second.

## future improvements:

* implement batch-importer CSV "API" on top of this
* stripe writes across store-files (i.e. strip the relationship-record file over 10 handlers, according to CPUs)
* parallelize writing to dynamic string and arraystore too
* change relationship-record updates for backwards pointers to run in a separate handler that is
  RandomAccessFile-based (or nio2) and just writes the 2 int values directly at file-pos
* add a csv analyser / sorter that
* add support & parallelize index addition
* good support for index based lookup for relationship construction (kv-store, better in-memory structure, e.g. a collection of long[])
* use id-compression internally to save memory in structs (write a CompressedLongArray)
* reuse PropertyBlock, PropertyRecords, RelationshipRecords, NodeRecords, probably subclass them and override getId() etc. or copy the code
  from the Store's to work with interfaces