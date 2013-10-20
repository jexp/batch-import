package org.neo4j.batchimport.importer;

import org.neo4j.batchimport.LineData;

import java.util.Collections;
import java.util.HashMap;
import java.util.Map;
import java.util.StringTokenizer;

import static org.neo4j.helpers.collection.MapUtil.map;

public class RowData implements LineData {
    private Object[] properties;
    private final int offset;
    private final String delim;
    private final String[] lineData;
    private final int lineSize;
    private int rows;
    int labelId = 2;
    int explicitLabelId = 2;
    private LineData.Header[] headers;
    private int propertyCount;
    private boolean hasIndex=false;
    private boolean hasId;

    public RowData(String header, String delim, int offset) {
        this.offset = offset;
        this.delim = delim;
        String[] fields = header.split(delim);
        lineSize = fields.length;
        lineData = new String[lineSize];
        this.headers = createHeaders(fields);
        createMapData(lineSize, offset);
    }

    private Header[] createHeaders(String[] fields) {
        Header[] headers = new Header[fields.length];
        for (int i = 0; i < fields.length; i++) {
            String[] parts=fields[i].split(":");
            final String name = parts[0];
            final String indexName = parts.length > 2 ? parts[2] : null;
            Type type = Type.fromString(parts.length > 1 ? parts[1] : null);
            if (type==Type.LABEL || name.toLowerCase().matches("^(type|types|label|labels)$")) {
                labelId=i;
                type=Type.LABEL;
                explicitLabelId=i;
            }
            headers[i]=new Header(i, name, type, indexName);
            hasIndex |= indexName != null;
        }
        hasId = headers[0].type == Type.ID;
        return headers;
    }

    private Object[] createMapData(int lineSize, int offset) {
        int dataSize = Math.max(0,lineSize - offset);
        properties = new Object[dataSize*2];
        for (int i = offset; i < dataSize; i++) {
            properties[(i - offset) * 2 ] = headers[i].name;
        }
        return properties;
    }

    @Override
    public boolean processLine(String line) {
        this.propertyCount = parse(line);
        return true;
    }

    @Override
    public Header[] getHeader() {
        return headers;
    }

    @Override
    public long getId() {
        if (hasId) return (Long)getValue(0);
        return rows;
    }

    @Override
    public boolean hasId() {
        return hasId;
    }

    @Override
    public Map<String, Object> getProperties() {
        return properties();
    }

    @Override
    public Map<String, Map<String, Object>> getIndexData() {
        if (!hasIndex) return Collections.EMPTY_MAP;
        Map<String, Map<String, Object>> indexData = new HashMap<String, Map<String, Object>>();
        for (int column = 0; column < headers.length; column++) {
            Header header = headers[column];
            if (header.indexName == null) continue;

            if (!indexData.containsKey(header.indexName)) {
                indexData.put(header.indexName, new HashMap<String, Object>());
            }
            indexData.get(header.indexName).put(header.name,getValue(column));
        }
        return indexData;
    }

    @Override
    public String[] getTypeLabels() {
        if (explicitLabelId==-1) return null;
        Object labels = getValue(explicitLabelId);
        return labels instanceof String ? new String[]{ labels.toString() } : (String[]) labels;
    }

    @Override
    public String getRelationshipTypeLabel() {
        Object labels = getValue(labelId);
        return labels instanceof String[] ? ((String[])labels)[0] : (String)labels;
    }

    @Override
    public Object getValue(int column) {
        return getHeader(column).type.convert(lineData[column]);
    }

    private Header getHeader(int column) {
        return headers[column];
    }

    private int parse(String line) {
        rows++;
        final StringTokenizer st = new StringTokenizer(line, delim,true);
        for (int i = 0; i < lineSize; i++) {
            String value = st.hasMoreTokens() ? st.nextToken() : delim;
            if (value.equals(delim)) {
                lineData[i] = null;
            } else {
                lineData[i] = value.trim().isEmpty() ? null : value;
                if (i< lineSize -1 && st.hasMoreTokens()) st.nextToken();
            }
        }
        return collectNonNullInData();
    }

    private int collectNonNullInData() {
        int count = 0;
        for (int i = offset; i < lineSize; i++) {
            if (lineData[i] == null) continue;
            final Header header = getHeader(i);
            properties[count++]= header.name;
            properties[count++]= getValue(i);
        }
        return count;
    }

    public Map<String,Object> updateMap(String line, Object... header) {
        processLine(line);

        // todo deprecate
        if (header.length > 0) {
            System.arraycopy(lineData, 0, header, 0, header.length);
        }

        return properties();
    }

    private Map<String, Object> properties() {
        if (propertyCount == properties.length) {
            return map(properties);
        }
        Object[] newData=new Object[propertyCount];
        System.arraycopy(properties,0,newData,0, propertyCount);
        return map(newData);
    }

    public int getColumnCount() {
        return this.propertyCount/2;
    }
}
