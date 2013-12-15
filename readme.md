# Neo4j (CSV) Batch Importer

This software is licensed under the [GPLv3](http://www.gnu.org/licenses/gpl-3.0.en.html) for now. 
You can ask [Neo Technology](http://neotechnology.com) about a different licensing agreement.

__Works with Neo4j 2.0.0__

## Note: The initial reference node is gone in Neo4j 2.0, so node-id numbering changed, see below

To simply use it:
* [download zip](https://dl.dropboxusercontent.com/u/14493611/batch_importer_20.zip)
* unzip
* run `import.sh test.db nodes.csv rels.csv` (on Windows: `import.bat`)

You provide one **tab separated** csv file for nodes and one for relationships (optionally more for indexes)

Example data for the files is a small family network

## File format

* **tab separated** csv files
* Property names in first row.
* If only one file is initially imported, the row number corresponds to the node-id (*starting with 0*)
* Property values not listed will not be set on the nodes or relationships.
* Optionally property fields can have a type (defaults to String) indicated with name:type where type is one of
  (int, long, float, double, boolean, byte, short, char, string). The string value is then converted to that type.
  Conversion failure will result in abort of the import operation.
* There is a separate "label" type, which should be used for relationship types and/or node labels, (`labels:label`)
* Property fields may also be arrays by adding "_array" to the types above and separating the data with commas.
* for non-ascii characters make sure to add `-Dfile.encoding=UTF-8` to the commandline arguments
* Optionally automatic indexing of properties can be configured with a header like `name:string:users` and a configured index in `batch.properties` like `batch_import.node_index=exact`
  then the property `name` will be indexed in the `users` index for each row with a value there
* multiple files for nodes and rels, comma separated, without spaces like "node1.csv,node2.csv"
* you can specify concrete, externally provided node-id's with: `i:id`, both in the node and relationship-files
* csv files can be zipped individually as *.gz or *.zip

## Examples

There is also a `sample` directory, please run from the main directory `./import.sh test.db sample/nodes.csv sample/rels.csv`

### nodes.csv

    name    l:label       age works_on
    Michael Person,Father 37  neo4j
    Selina  Person,Child  14
    Rana    Person,Child  6
    Selma   Person,Child  4

### rels.csv

Note that the node-id references are numbered from 0 (since Neo4j 2.0)

    start	end	type	    since   counter:int
    0     1   FATHER_OF	1998-07-10  1
    0     2   FATHER_OF 2007-09-15  2
    0     3   FATHER_OF 2008-05-03  3
    2     3   SISTER_OF 2008-05-03  5
    1     2   SISTER_OF 2007-09-15  7


## Execution

Just use the provided shell script `import.sh` or `import.bat` on Windows

    import.sh test.db nodes.csv rels.csv


### For Developers

If you want to work on the code and run the importer after making changes:

    mvn clean compile exec:java -Dexec.mainClass="org.neo4j.batchimport.Importer" -Dexec.args="test.db nodes.csv rels.csv"

    ynagzet:batchimport mh$ java -server -Xmx4G -jar target/batch-import-jar-with-dependencies.jar target/db nodes.csv rels.csv
    Physical mem: 16384MB, Heap size: 3640MB

    Configuration:
    use_memory_mapped_buffers=false
    neostore.nodestore.db.mapped_memory=200M
    neostore.relationshipstore.db.mapped_memory=1000M
    neostore.propertystore.db.mapped_memory=1000M
    neostore.propertystore.db.strings.mapped_memory=100M
    neostore.propertystore.db.arrays.mapped_memory=215M
    neo_store=/Users/mh/java/neo/batchimport/test.db
    dump_configuration=true
    cache_type=none

    ...........................................................................
    Importing 7500000 Nodes took 17 seconds
    ....................................................................................................35818 ms
    ....................................................................................................39343 ms
    ....................................................................................................41788 ms
    ....................................................................................................48897 ms
    ............
    Importing 41246740 Relationships took 170 seconds
    Total 212 seconds
    ynagzet:batchimport mh$ du -sh test.db
    3,2G	test.db

## Parameters

*First parameter* MIGHT be the property-file name, if so it has to end with `.properties`, then this file will be used and all other parameters are consumed as usual

*First parameter* - the graph database directory, a new db will be created in the directory except when `batch_import.keep_db=true` is set in `batch.properties`.

*Second parameter* - a comma separated list of *node-csv-files*

*Third parameter* - a comma separated list of *relationship-csv-files*

It is also possible to specify those two file-lists in the config:

````
batch_import.nodes_files=nodes1.csv[,nodes2.csv]
batch_import.rels_files=rels1.csv[,rels2.csv]
````

*Fourth parameter* - index configuration each a set of 4 values: `node_index users fulltext nodes_index.csv` or more generally: `node-or-rel-index index-name index-type index-file`

This parameter set can be repeatedly used, see below. It is also possible to configure this in the config (`batch.properties`)

````
batch_import.node_index.users=exact
````

## Schema indexes

Currently schema indexes are not created by the batch-inserter, you could create them upfront and use `batch_import.keep_db=true` to work with the existing database.
You then have the option of specifying labels for your nodes using a column header like `type:label` and a comma separated list of label values.
Then on shutdown of the import Neo4j will populate the schema indexes with nodes with the appropriate labels and properties automatically.
(The index creation is As a rough estimate the index creation will

## (Legacy) Indexing

### Automatic Indexing

You can automatically index properties of nodes and relationships by adding ":indexName" to the property-header.
Just configure the indexes in `batch.properties` like so:

If you use `node_auto_index` as the index name, you can also initially populate the automatic node index which is then
later used and and updated by Neo4j.

````
batch_import.node_index.users=exact
````

````
name:string:users    age works_on
Michael 37  neo4j
Selina  14
Rana    6
Selma   4
````

In the relationships-file you can optionally specify that the start and end-node should be looked up from the index in the same way

````
name:string:users	name:string:users	type	    since   counter:int
Michael     Selina   FATHER_OF	1998-07-10  1
Michael     Rana   FATHER_OF 2007-09-15  2
Michael     Selma   FATHER_OF 2008-05-03  3
Rana     Selma   SISTER_OF 2008-05-03  5
Selina     Rana   SISTER_OF 2007-09-15  7
````

### Explicit Indexing

Optionally you can add nodes and relationships to indexes.

Add four arguments per each index to command line:

To create a full text node index called users using nodes_index.csv:

````
node_index users fulltext nodes_index.csv
````

To create an exact relationship index called worked using rels_index.csv:

````
rel_index worked exact rels_index.csv
````

Example command line:

````
./import.sh test.db nodes.csv rels.csv node_index users fulltext nodes_index.csv rel_index worked exact rels_index.csv
````
## Examples

### nodes_index.csv

````
id	name	language
0	Victor Richards	West Frisian
1	Virginia Shaw	Korean
2	Lois Simpson	Belarusian
3	Randy Bishop	Hiri Motu
4	Lori Mendoza	Tok Pisin
````

### rels_index.csv

````
id	property1	property2
0	cwqbnxrv	rpyqdwhk
1	qthnrret	tzjmmhta
2	dtztaqpy	pbmcdqyc
````

## Configuration

The Importer uses a supplied `batch.properties` file to be configured:

#### Memory Mapping I/O Config

Most important is the memory config, you should try to have enough RAM map as much of your store-files to memory as possible.

At least the node-store and large parts of the relationship-store should be mapped. The property- and string-stores are mostly
append only so don't need that much RAM. Below is an example for about 6GB RAM, to leave room for the heap and also OS and OS caches.

````
cache_type=none
use_memory_mapped_buffers=true
# 9 bytes per node
neostore.nodestore.db.mapped_memory=200M
# 33 bytes per relationships
neostore.relationshipstore.db.mapped_memory=3G
# 38 bytes per property
neostore.propertystore.db.mapped_memory=500M
# 60 bytes per long-string block
neostore.propertystore.db.strings.mapped_memory=500M
neostore.propertystore.db.index.keys.mapped_memory=5M
neostore.propertystore.db.index.mapped_memory=5M
````

#### Indexes (experimental)

````
batch_import.node_index.users=exact
batch_import.node_index.articles=fulltext
batch_import.relationship_index.friends=exact
````

#### CSV (experimental)

````
batch_import.csv.quotes=true // default, set to false for faster, experimental csv-reader
batch_import.csv.delim=,
````

##### Index-Cache (experimental)

````
batch_import.mapdb_cache.disable=true
````

##### Keep Database (experimental)

````
batch_import.keep_db=true
````

# Parallel Batch Inserter with Neo4j

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

* have to know max # of rels per node, properties per node and relationship
* relationships have to be pre-sorted by min(start,end)

## measurements

We successfully imported 2bn nodes (2 properties) and 20bn relationships (1 property) in 11 hours on an EC2 high-IO instance,
with 35 ECU, 60GB RAM, 2TB SSD writing up to 500MB/s, resulting in a store of 1.4 TB. That makes around 500k elements per second.

## future improvements:

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


## Utilities

### TestDataGenerator

It is a dumb random test data generator (`org.neo4j.batchimport.TestDataGenerator`) that you can run with

./generate.sh #nodes #max-rels-per-node REL1,REL2,REL3 LABEL1,LABEL2,LABEL3

Will generate nodes.csv and rels.csv for those numbers


### Relationship-Sorter

Sorts a given relationship-CSV file by min(start,end) as required for the parallel sorter. Uses the data-pump sorter from mapdb
for the actual sorting with a custom Comparator.

`org.neo4j.batchimport.utils.RelationshipSorter` rels-input.csv rels-output.csv


