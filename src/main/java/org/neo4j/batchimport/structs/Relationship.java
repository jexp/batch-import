package org.neo4j.batchimport.structs;

/**
* @author mh
* @since 27.10.12
*/
public class Relationship extends PropertyHolder {
    // encode outgoing > 0, incoming as 2-complement ~other
    public volatile long other;
    public volatile int type;

    public Relationship(int propertyCount) {
        super(propertyCount);
    }

    public Relationship init(long other, boolean outgoing, int type) {
        super.init();
        this.other = outgoing ? other : ~other;
        this.type = type;
        return this;
    }
    public boolean outgoing() {
        return other >= 0;
    }

    public long other() {
        return other < 0 ? ~other : other;
    }

    @Override
    public String toString() {
        return String.format("Rel[%d] %s-[%d]->%s %s",id, outgoing() ? "?" : other(),type,outgoing() ? other() : "?",outgoing());
    }
}
