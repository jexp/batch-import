package org.neo4j.batchimport.importer;

import au.com.bytecode.opencsv.CSVReader;

import java.io.IOException;
import java.io.Reader;

import static org.neo4j.helpers.collection.MapUtil.map;

public class CsvLineData extends AbstractLineData {
    private final CSVReader csvReader;

    public CsvLineData(Reader reader, char delim, int offset) {
        super(offset);
        this.csvReader = new CSVReader(reader, delim,'"','\\',0,false,false);
        initHeaders(createHeaders(readRawRow()));
        createMapData(lineSize, offset);
    }

    @Override
    protected String[] readRawRow() {
        try {
            return csvReader.readNext();
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
    }

    protected boolean readLine() {
        final String[] row = readRawRow();
        if (row==null || row.length==0) return false;
        for (int i = 0; i < row.length && i < lineSize; i++) {
            String value = row[i];
            if (value != null && !value.isEmpty()) {
                lineData[i] = headers[i].type == Type.STRING ? value : headers[i].type.convert(value);
            } else {
                lineData[i] = null;
            }
        }
        return true;
    }

}
