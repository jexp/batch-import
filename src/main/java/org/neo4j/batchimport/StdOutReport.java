package org.neo4j.batchimport;

public class StdOutReport implements Report {
    private final long batch;
    private final long dots;
    private long count;
    private long total = System.currentTimeMillis(), time, batchTime;

    public StdOutReport(long batch, int dots) {
        this.batch = batch;
        this.dots = batch / dots;
    }

    @Override
    public void reset() {
        count = 0;
        batchTime = time = System.currentTimeMillis();
    }

    @Override
    public void finish() {
        System.out.println("\nTotal import time: "+ (System.currentTimeMillis() - total) / 1000 + " seconds ");
    }

    @Override
    public void dots() {
        if ((++count % dots) != 0) return;
        System.out.print(".");
        if ((count % batch) != 0) return;
        long now = System.currentTimeMillis();
        System.out.println(" "+ (now - batchTime) + " ms for "+batch);
        batchTime = now;
    }

    public long getCount() {
        return count;
    }

    @Override
    public void finishImport(String type) {
        System.out.println("\nImporting " + count + " " + type + " took " + (System.currentTimeMillis() - time) / 1000 + " seconds ");
    }
}
