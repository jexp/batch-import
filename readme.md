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

   mvn exec:java -Dexec.mainClass=org.neo4j.batchimport.DisruptorTest

current limitations, constraints:

* only up to 2bn relationships (due to an int based multi-map)
* have to know max # of rels per node, properties per node and relationship
* relationships have to be pre-sorted by min(start,end)
