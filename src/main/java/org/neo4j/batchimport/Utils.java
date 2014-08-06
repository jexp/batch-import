package org.neo4j.batchimport;

import org.apache.log4j.Logger;

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

    static String join(String[] types, String delim) {
        StringBuilder sb =new StringBuilder();
        for (String type : types) {
            sb.append(type).append(delim);
        }
        return sb.substring(0, sb.length() - delim.length());
    }
}
