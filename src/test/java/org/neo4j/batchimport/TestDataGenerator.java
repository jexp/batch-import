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

    private static final int NODES = 1  * 1000 * 1000;
    private static final int RELS_PER_NODE = 50;
    private static final String[] TYPES = {"ONE","TWO","THREE","FOUR","FIVE","SIX","SEVEN","EIGHT","NINE","TEN"};

    public static void main(String...args) throws IOException {
        System.out.println("Usage: TestDataGenerator NODES RELS_PER_NODE TYPE1,TYPE2,TYPE3 sorted");
        long relCount=0, time = System.currentTimeMillis();

        int nodes = args.length > 0 ? Integer.parseInt(args[0]) : NODES;
        int relsPerNode = args.length > 1 ? Integer.parseInt(args[1]) : RELS_PER_NODE;
        String[] types = args.length > 2 ? args[2].split(",") : TYPES;
        final boolean sorted = args.length > 0 && args[args.length-1].equalsIgnoreCase("sorted");
        System.out.println("Using: TestDataGenerator "+nodes+" "+relsPerNode+" "+ Utils.join(types, ",")+" "+(sorted?"sorted":""));

        BufferedWriter nodeFile = new BufferedWriter(new FileWriter("nodes.csv"));
        nodeFile.write("Node\tRels\tProperty\tLabel:label\tCounter:int\n");
        BufferedWriter relFile = new BufferedWriter(new FileWriter("rels.csv"));
        relFile.write("Start\tEnde\tType\tProperty\tCounter:long\n");

        if (sorted) {
            relCount = generateSortedRels(relCount, nodeFile, relFile, nodes, relsPerNode, types);
        } else {
            relCount = generateRandomRels(relCount, nodeFile, relFile, nodes, relsPerNode, types);
        }
        nodeFile.close();
        relFile.close();
        long seconds = (System.currentTimeMillis() - time) / 1000;
        System.out.println("Creating " + nodes + " Nodes and " + relCount + (sorted? " sorted " : "") + " Relationships took " + seconds + " seconds.");
    }

    private static long generateRandomRels(long relCount, BufferedWriter nodeFile, BufferedWriter relFile, int nodes, int relsPerNode, String[] types) throws IOException {
        Random rnd = new Random();
        int numTypes = types.length;
        for (int node = 0; node < nodes; node++) {
            final int rels = rnd.nextInt(relsPerNode);
            nodeFile.write(node+"\t"+rels+"\tTEST\t"+types[node % numTypes]+"\t"+node+"\n");
            for (int rel = rels; rel >= 0; rel--) {
                relCount++;
                final int node1 = rnd.nextInt(nodes);
                final int node2 = rnd.nextInt(nodes);
                relFile.write(node1 + "\t" + node2 + "\t" + types[rel % numTypes] + "\t" + "Property"+"\t" + relCount+ "\n");
            }
        }
        return relCount;
    }
    private static long generateSortedRels(long relCount, BufferedWriter nodeFile, BufferedWriter relFile, int nodes, int relsPerNode, String[] types) throws IOException {
        Random rnd = new Random();
        int numTypes = types.length;
        for (int node = 0; node < nodes; node++) {
            final int rels = rnd.nextInt(relsPerNode);
            nodeFile.write(node+"\t"+rels+"\tTEST\t"+node+"\n");
            for (int rel = rels; rel >= 0; rel--) {
                relCount++;
                final int target = node + rnd.nextInt(nodes -node);
                final boolean outgoing = rnd.nextBoolean();
                if (outgoing) {
                    relFile.write(node + "\t" + target + "\t" + types[rel % numTypes] + "\t" + "Property"+"\t" + relCount+ "\n");
                } else {
                    relFile.write(target + "\t" + node + "\t" + types[rel % numTypes] + "\t" + "Property"+"\t" + relCount+ "\n");
                }
            }
        }
        return relCount;
    }
}
