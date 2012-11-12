package org.neo4j.batchimport;

import org.apache.log4j.Logger;
import org.neo4j.batchimport.structs.NodeStruct;
import org.neo4j.helpers.collection.MapUtil;
import org.neo4j.kernel.impl.nioneo.store.NodeRecord;
import org.neo4j.kernel.impl.nioneo.store.RelationshipRecord;

import java.io.File;
import java.io.FileWriter;
import java.util.HashMap;
import java.util.Map;

/**
 * @author mh
 * @since 27.10.12
 */
public class Utils {
    private final static Logger log = Logger.getLogger(Utils.class);

    public static int size(int[] ids) {
        if (ids==null) return 0;
        int count = ids.length;
        for (int i=count-1;i>=0;i--) {
            if (ids[i]!=-1) return i+1;
        }
        return count;
    }

    public static int size(long[] ids) {
        if (ids==null) return 0;
        int count = ids.length;
        for (int i=count-1;i>=0;i--) {
            if (ids[i]!=-1) return i+1;
        }
        return count;
    }

    private static void printRelationship(RelationshipRecord record) {
        if (log.isDebugEnabled()) log.debug(formatRecord(record));
    }

    private static String formatRecord(RelationshipRecord record) {
        return String.format("Rel[%d] %s-[%d]->%s created %s chain start: %d->%d target %d->%d", record.getId(), record.getFirstNode(), record.getType(), record.getSecondNode(), record.isCreated(), record.getFirstPrevRel(), record.getFirstNextRel(), record.getSecondPrevRel(), record.getSecondNextRel());
    }

    private static void printNode(NodeStruct record) {
        if (log.isDebugEnabled()) log.debug(formatNode(record));
    }

    private static String formatNode(NodeStruct record) {
        return String.format("Node[%d] -> %d, .%d", record.id, record.firstRel, record.firstPropertyId);
    }

    private static String formatNode(NodeRecord record) {
        return String.format("Node[%d] -> %d, .%d", record.getId(), record.getNextRel(), record.getNextProp());
    }

    static Map<String, String> config() {
        Map<String, String> config = new HashMap<String, String>();
        try {
            if (new File("batch.properties").exists()) {
                System.out.println("Using Existing Configuration File");
            } else {
                System.out.println("Writing Configuration File to batch.properties");
                FileWriter fw = new FileWriter("batch.properties");
                fw.append("use_memory_mapped_buffers=true\n"
                        + "neostore.nodestore.db.mapped_memory=100M\n"
                        + "neostore.relationshipstore.db.mapped_memory=500M\n"
                        + "neostore.propertystore.db.mapped_memory=1G\n"
                        + "neostore.propertystore.db.strings.mapped_memory=200M\n"
                        + "neostore.propertystore.db.arrays.mapped_memory=0M\n"
                        + "neostore.propertystore.db.index.keys.mapped_memory=15M\n"
                        + "neostore.propertystore.db.index.mapped_memory=15M");
                fw.close();
            }

            config = MapUtil.load(new File("batch.properties"));

        } catch (Exception e) {
            System.out.println(e.getMessage());
        }
        return config;
    }
}
