package org.neo4j.batchimport.importer;

import org.neo4j.batchimport.LineData;

import java.util.Collections;
import java.util.HashMap;
import java.util.Map;

import static org.neo4j.helpers.collection.MapUtil.map;

public abstract class AbstractLineData implements LineData {
    protected final int offset;
    protected Object[] lineData;
    protected int lineSize;
    protected Header[] headers;
    int labelId = 2;
    private Object[] properties;
    private int rows;
    private int propertyCount;
    private boolean hasIndex=false;
    private boolean done;

    public AbstractLineData(int offset) {
        this.offset = offset;
    }

    protected void initHeaders(Header[] headers) {
        this.headers = headers;
        lineSize=headers.length;
        lineData = new Object[lineSize];
    }
    protected abstract String[] readRawRow();

    protected abstract boolean readLine();

    protected Header[] createHeaders(String[] fields) {
        Header[] headers = new Header[fields.length];
        int i=0;
        for (String field : fields) {
            String[] parts=field.split(":");
            final String name = parts[0];
            final String indexName = parts.length > 2 ? parts[2] : null;
            Type type = Type.fromString(parts.length > 1 ? parts[1] : null);
            if (type==Type.LABEL || name.toLowerCase().matches("^(type|types|label|labels)$")) {
                labelId=i;
                type=Type.LABEL;
            }
            headers[i]=new Header(i, name, type, indexName);
            i++;
            hasIndex |= indexName != null;
        }
        return headers;
    }

    protected Object[] createMapData(int lineSize, int offset) {
        int dataSize = Math.max(0,lineSize - offset);
        properties = new Object[dataSize*2];
        for (int i = offset; i < dataSize; i++) {
            properties[(i - offset) * 2 ] = headers[i].name;
        }
        return properties;
    }

    @Override
    public boolean processLine(String line) {
        if (done) return false;
        this.propertyCount = parse();
        return true;
    }

    @Override
    public Header[] getHeader() {
        return headers;
    }

    @Override
    public long getId() {
        return rows;
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
        return (String[])getValue(labelId);
    }

    @Override
    public Object getValue(int column) {
        return lineData[column];
    }

    private Header getHeader(int column) {
        return headers[column];
    }

    private int parse() {
        rows++;
        done = !readLine();
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

    public Map<String,Object> updateMap(Object... header) {
        processLine(null);

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
