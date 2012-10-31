Parallel Batch inserter with Neo4j

Uses the [LMAX Disruptor](http://lmax-exchange.github.com/disruptor/) to parallelize operations during batch-insertion.

The 6 operations are:

1. property encoding
2. property-record creation
3. relationship-id creation and forward handling of reverse relationship chains
4. writing node-records
5. writing relationship-records
6. writing property-records


Dependencies:

    (1)<--(2)<--(6)
    (2)<--(5)-->(3)   
    (2)<--(4)-->(3)   

It uses the above dependency setup of disruptor handlers to execute the different concerns in parallel. A ringbuffer of about 2^18 elements is used and a heap size of 5-20G, MMIO configuration within the heap limits.

Execution:

   MAVEN_OPTS="-Xmx5G -Xms5G -server -d64 -XX:NewRatio=5"  mvn exec:java -Dexec.mainClass=org.neo4j.batchimport.DisruptorTest

current limitations, constraints:

* only up to 2bn relationships (due to an int based multi-map)
* have to know max # of rels per node, properties per node and relationship
* relationships have to be pre-sorted by min(start,end)


future improvements:

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