MEMORY_OPTS="-Xmx50G -Xms50G -server -d64 -Xmn3g -XX:SurvivorRatio=2"
GC_OPTS="-XX:+UseConcMarkSweepGC -XX:+UseParNewGC -XX:ParallelCMSThreads=4 -XX:+CMSParallelRemarkEnabled -XX:+CMSIncrementalMode -XX:+CMSIncrementalPacing -XX:CMSIncrementalDutyCycle=10 -XX:CMSFullGCsBeforeCompaction=1 "

PRINT_GC_OPTS="-XX:+PrintTenuringDistribution -XX:+PrintGCDetails -XX:+PrintGCDateStamps -Xloggc:gc.log"

# PROFILE_OPTS="-agentpath:/root/yourkit/bin/linux-x86-64/libyjpagent.so=port=10001"

#-XX:+PrintGCApplicationStoppedTime -XX:+PrintGCApplicationConcurrentTime -XX:+PrintHeapAtGC -XX:+PrintGCTaskTimeStamps

export MAVEN_OPTS="$PROFILE_OPTS $MEMORY_OPTS $GC_OPTS $PRINT_GC_OPTS"