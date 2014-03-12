package org.neo4j.batchimport.index;

import org.neo4j.graphdb.ResourceIterator;
import org.neo4j.graphdb.index.IndexHits;
import org.neo4j.helpers.collection.IteratorUtil;

import java.util.Iterator;

/**
* @author mh
* @since 11.06.13
*/
public class LongIterableIndexHits implements IndexHits<Long>, ResourceIterator<Long> {

    private final Iterable<Long> values;
    private Iterator<Long> iterator;

    public LongIterableIndexHits(Iterable<Long> values) {
        this.values = values;
        iterator = iterator();
    }

    @Override
    public int size() {
        return IteratorUtil.count(values);
    }

    @Override
    public void close() {
        if (iterator instanceof ResourceIterator) {
            ((ResourceIterator)iterator).close();
        }
    }

    @Override
    public Long getSingle() {
        return IteratorUtil.singleOrNull(values);
    }

    @Override
    public float currentScore() {
        return 0;
    }

    @Override
    public ResourceIterator<Long> iterator() {
        iterator = values.iterator();
        return this;
    }


    @Override
    public boolean hasNext() {
        return iterator.hasNext();
    }

    @Override
    public Long next() {
        return iterator.next();
    }

    @Override
    public void remove() {
        iterator.remove();
    }
}
