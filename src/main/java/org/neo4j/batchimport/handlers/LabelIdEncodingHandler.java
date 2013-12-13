package org.neo4j.batchimport.handlers;

import com.lmax.disruptor.EventHandler;
import org.neo4j.batchimport.structs.NodeStruct;
import org.neo4j.batchimport.structs.Relationship;
import org.neo4j.kernel.impl.nioneo.store.DynamicRecord;
import org.neo4j.kernel.impl.util.Bits;

import java.util.Arrays;
import java.util.Collection;

import static java.lang.Long.highestOneBit;
import static org.neo4j.kernel.impl.util.Bits.bits;

/**
 * @author mh
 * @since 27.10.12
 */
public class LabelIdEncodingHandler implements EventHandler<NodeStruct> {

    private long counter;

    public void onEvent(NodeStruct event, long nodeId, boolean endOfBatch) throws Exception {
//        if (event.labelCount == 0) return;
//        long encoded = encodeLabels(event.labelIds, event.labelCount);
//        if (encoded == -1)
//            throw new RuntimeException("Cannot encode labels for node " + nodeId + " " + Arrays.toString(event.labelIds));
//        event.labelField = encoded;
        counter++;
    }

    @Override
    public String toString() {
        return "labelEncoding: " + counter;
    }

    public static final int LABEL_BITS = 36;

    public static long encodeLabels(int[] ids, int labelCount) {
        // We reserve the high header bit for future extensions of the format of the in-lined label bits
        // i.e. the 0-valued high header bit can allow for 0-7 in-lined labels in the bit-packed format.
        if (labelCount > 7) return -1;

        byte bitsPerLabel = (byte) (LABEL_BITS / labelCount);
        long limit = 1 << bitsPerLabel;

        if (labelCount > 1) Arrays.sort(ids,0,labelCount);
        long value = 0L;
        for (int i = 0 ; i < labelCount; i++) {
            if (highestOneBit(ids[i]) >= limit) return -1;
            value |= ids[i];
            if (i < labelCount-1) value <<= bitsPerLabel;
        }
        return combineLabelCountAndLabelStorage((byte) labelCount, value);
    }

    private static long combineLabelCountAndLabelStorage(byte labelCount, long labelBits) {
        return (((long) labelCount << 36) | labelBits);
    }

}
