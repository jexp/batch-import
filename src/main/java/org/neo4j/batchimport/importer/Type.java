package org.neo4j.batchimport.importer;

public enum Type {
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
}
