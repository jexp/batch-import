package org.neo4j.batchimport;

import org.junit.Ignore;

import java.io.BufferedWriter;
import java.io.FileWriter;
import java.io.IOException;
import java.util.Random;

/**
 * @author mh
 * @since 13.01.12
 */
@Ignore
public class TestDataGenerator {

    private static int NODES = 1  * 1000; // * 1000;
    private static final int RELS_PER_NODE = 50;
    private static final String[] TYPES = {"ONE","TWO","THREE","FOUR","FIVE","SIX","SEVEN","EIGHT","NINE","TEN"};
    public static final int NUM_TYPES = 10;

    public static void main(String...args) throws IOException {
        long relCount=0, time = System.currentTimeMillis();
        BufferedWriter nodeFile = new BufferedWriter(new FileWriter("nodes.csv"));
        nodeFile.write("Node\tRels\tProperty\tCounter:int\n");
        BufferedWriter relFile = new BufferedWriter(new FileWriter("rels.csv"));
        relFile.write("Start\tEnde\tType\tProperty\tCounter:long\n");
        final boolean sorted = args.length > 0 && args[0].equalsIgnoreCase("sorted");
        if (sorted) {
            relCount = generateSortedRels(relCount, nodeFile, relFile);
        } else {
            relCount = generateRandomRels(relCount, nodeFile, relFile);
        }
        nodeFile.close();
        relFile.close();
        System.out.println("Creating " + NODES + " and " + relCount + (sorted? " sorted " : "") + " Relationships took " + ((System.currentTimeMillis() - time) / 1000) + " seconds.");
    }

    private static long generateRandomRels(long relCount, BufferedWriter nodeFile, BufferedWriter relFile) throws IOException {
        Random rnd = new Random();
        for (int node = 0; node < NODES; node++) {
            final int rels = rnd.nextInt(RELS_PER_NODE);
            nodeFile.write(node+"\t"+rels+"\tTEST\t"+node+"\n");
            for (int rel = rels; rel >= 0; rel--) {
                relCount++;
                final int node1 = rnd.nextInt(NODES);
                final int node2 = rnd.nextInt(NODES);
                relFile.write(node1 + "\t" + node2 + "\t" + TYPES[rel % NUM_TYPES] + "\t" + "Property"+"\t" + relCount+ "\n");
            }
        }
        return relCount;
    }
    private static long generateSortedRels(long relCount, BufferedWriter nodeFile, BufferedWriter relFile) throws IOException {
        Random rnd = new Random();
        for (int node = 0; node < NODES; node++) {
            final int rels = rnd.nextInt(RELS_PER_NODE);
            nodeFile.write(node+"\t"+rels+"\tTEST\t"+node+"\n");
            for (int rel = rels; rel >= 0; rel--) {
                relCount++;
                final int target = node + rnd.nextInt(NODES-node);
                final boolean outgoing = rnd.nextBoolean();
                if (outgoing) {
                    relFile.write(node + "\t" + target + "\t" + TYPES[rel % NUM_TYPES] + "\t" + "Property"+"\t" + relCount+ "\n");
                } else {
                    relFile.write(target + "\t" + node + "\t" + TYPES[rel % NUM_TYPES] + "\t" + "Property"+"\t" + relCount+ "\n");
                }
            }
        }
        return relCount;
    }
}
