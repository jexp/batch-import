package org.neo4j.batchimport.utils;

import java.io.File;

/**
 * @author mh
 * @since 02.11.12
 */
public class Params {

    private final String names;
    private final String[] args;
    private String[] params;

    public Params(String names, String... args) {
        this.names = names;
        this.params = names.split(" +");
        this.args = args;
    }

    public boolean invalid() {
        return args.length != params.length;
    }

    public int length() {
        return params.length;
    }

    @Override
    public String toString() {
        return names;
    }

    public File file(String name) {
        return new File(string(name));
    }

    public String string(String name) {
        for (int i = 0; i < params.length; i++) {
            if (params[i].equalsIgnoreCase(name)) {
                return args[i];
            }
        }
        throw new IllegalArgumentException("Invalid name" + name + " only know " + names);
    }

    public long longValue(String name) {
        return Long.parseLong(string(name));
    }

    public int intValue(String name) {
        return Integer.parseInt(string(name));
    }
}
