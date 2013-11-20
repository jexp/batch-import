package org.neo4j.batchimport.importer;

import org.neo4j.batchimport.utils.Config;

public enum Type {
    ID {
        @Override
        public Object convert(String value) {
            return Long.parseLong(value);
        }
        public boolean isProperty() { return false; }
    },
    LABEL {
        @Override
        public Object convert(String value) {
            return value.trim().split("\\s*,\\s*");
        }
        public boolean isProperty() { return false; }
    },
    BOOLEAN {
        @Override
        public Object convert(String value) {
            return Boolean.valueOf(value);
        }
    },
    INT {
        @Override
        public Object convert(String value) {
            return Integer.valueOf(value);
        }
    },
    LONG {
        @Override
        public Object convert(String value) {
            return Long.valueOf(value);
        }
    },
    DOUBLE {
        @Override
        public Object convert(String value) {
            return Double.valueOf(value);
        }
    },
    FLOAT {
        @Override
        public Object convert(String value) {
            return Float.valueOf(value);
        }
    },
    BYTE {
        @Override
        public Object convert(String value) {
            return Byte.valueOf(value);
        }
    },
    SHORT {
        @Override
        public Object convert(String value) {
            return Short.valueOf(value);
        }
    },
    CHAR {
        @Override
        public Object convert(String value) {
            return value.charAt(0);
        }
    },
    STRING {
        @Override
        public Object convert(String value) {
            return value;
        }
    },
    BOOLEAN_ARRAY {
        @Override
        public Object convert(String value) {
            String[] strArray = value.split(Config.ARRAYS_SEPARATOR);
            boolean[] booleanArray = new boolean[strArray.length];
            for(int i = 0; i < strArray.length; i++) {
                booleanArray[i] = Boolean.valueOf(strArray[i]);
            }
            return booleanArray;
        }
    },
    INT_ARRAY {
        @Override
        public Object convert(String value) {
            String[] strArray = value.split(Config.ARRAYS_SEPARATOR);
            int[] intArray = new int[strArray.length];
            for(int i = 0; i < strArray.length; i++) {
                intArray[i] = Integer.parseInt(strArray[i]);
            }
            return intArray;
        }
    },
    LONG_ARRAY {
        @Override
        public Object convert(String value) {
            String[] strArray = value.split(Config.ARRAYS_SEPARATOR);
            long[] longArray = new long[strArray.length];
            for(int i = 0; i < strArray.length; i++) {
                longArray[i] = Long.parseLong(strArray[i]);
            }
            return longArray;
        }
    },
    DOUBLE_ARRAY {
        @Override
        public Object convert(String value) {
            String[] strArray = value.split(Config.ARRAYS_SEPARATOR);
            double[] doubleArray = new double[strArray.length];
            for(int i = 0; i < strArray.length; i++) {
                doubleArray[i] = Double.parseDouble(strArray[i]);
            }
            return doubleArray;
        }
    },
    FLOAT_ARRAY {
        @Override
        public Object convert(String value) {
            String[] strArray = value.split(Config.ARRAYS_SEPARATOR);
            float[] floatArray = new float[strArray.length];
            for(int i = 0; i < strArray.length; i++) {
                floatArray[i] = Float.parseFloat(strArray[i]);
            }
            return floatArray;
        }
    },
    BYTE_ARRAY {
        @Override
        public Object convert(String value) {
            String[] strArray = value.split(Config.ARRAYS_SEPARATOR);
            byte[] byteArray = new byte[strArray.length];
            for(int i = 0; i < strArray.length; i++) {
                byteArray[i] = Byte.parseByte(strArray[i]);
            }
            return byteArray;
        }
    },
    SHORT_ARRAY {
        @Override
        public Object convert(String value) {
            String[] strArray = value.split(Config.ARRAYS_SEPARATOR);
            short[] shortArray = new short[strArray.length];
            for(int i = 0; i < strArray.length; i++) {
                shortArray[i] = Short.parseShort(strArray[i]);
            }
            return shortArray;
        }
    },
    CHAR_ARRAY {
        @Override
        public Object convert(String value) {
            String[] strArray = value.split(Config.ARRAYS_SEPARATOR);
            char[] charArray = new char[strArray.length];
            for(int i = 0; i < strArray.length; i++) {
                charArray[i] = strArray[i].charAt(0);
            }
            return charArray;
        }
    },
    STRING_ARRAY {
        @Override
        public Object convert(String value) {
            String separator = Config.ARRAYS_SEPARATOR;
            return value.split(Config.ARRAYS_SEPARATOR);
        }
    };

    public static Type fromString(String typeString) {
        if (typeString==null || typeString.isEmpty()) return Type.STRING;
        try {
            return valueOf(typeString.toUpperCase());
        } catch (Exception e) {
            throw new IllegalArgumentException("Unknown Type "+typeString);
        }
    }

    public abstract Object convert(String value);

    public boolean isProperty() { return true; }
}
