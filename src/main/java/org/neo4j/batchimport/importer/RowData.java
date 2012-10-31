package org.neo4j.batchimport.importer;

import java.util.Arrays;
import java.util.Map;
import java.util.StringTokenizer;

import static org.neo4j.helpers.collection.MapUtil.map;

public class RowData {
    private Object[] data;
    private final int offset;
    private final String delim;
    private final String[] fields;
    private final String[] lineData;
    private final Type types[];
    private final int lineSize;
    private int dataSize;

    public RowData(String header, String delim, int offset) {
        this.offset = offset;
        this.delim = delim;
        fields = header.split(delim);
        lineSize = fields.length;
        types = parseTypes(fields);
        lineData = new String[lineSize];
        createMapData(lineSize, offset);
    }

    private Object[] createMapData(int lineSize, int offset) {
        dataSize = lineSize - offset;
        data = new Object[dataSize*2];
        for (int i = 0; i < dataSize; i++) {
            data[i * 2] = fields[i + offset];
        }
        return data;
    }

    private Type[] parseTypes(String[] fields) {
        Type[] types = new Type[lineSize];
        Arrays.fill(types, Type.STRING);
        for (int i = 0; i < lineSize; i++) {
            String field = fields[i];
            int idx = field.indexOf(':');
            if (idx!=-1) {
               fields[i]=field.substring(0,idx);
               types[i]= Type.fromString(field.substring(idx + 1));
            }
        }
        return types;
    }

    private int split(String line) {
        final StringTokenizer st = new StringTokenizer(line, delim,true);
        int count=0;
        for (int i = 0; i < lineSize; i++) {
            String value = st.nextToken();
            if (value.equals(delim)) {
                lineData[i] = null;
            } else {
                lineData[i] = value.trim().isEmpty() ? null : value;
                if (i< lineSize -1) st.nextToken();
            }
            if (i >= offset && lineData[i]!=null) {
                data[count++]=fields[i];
                data[count++]=types[i].convert(lineData[i]);
            }
        }
        return count;
    }

    public Map<String,Object> update(String line, Object... header) {
        int nonNullCount = split(line);
        if (header.length > 0) {
            System.arraycopy(lineData, 0, header, 0, header.length);
        }

        if (nonNullCount == dataSize*2) {
            return map(data);
        }
        Object[] newData=new Object[nonNullCount];
        System.arraycopy(data,0,newData,0,nonNullCount);
        return map(newData);
    }

}
