package com.thinkaurelius.titan.hadoop.compat.h1;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;

import java.io.*;
import java.net.URL;
import java.util.Collection;
import java.util.Iterator;
import java.util.List;
import java.util.Map;

public class ImmutableConfiguration extends Configuration {

    private final Configuration encapsulated;

    public ImmutableConfiguration(final Configuration encapsulated) {
        this.encapsulated = encapsulated;
    }

    public static void addDefaultResource(String name) {
        throw new UnsupportedOperationException("This configuration instance is immutable");
    }

    @Override
    public void addResource(String name) {
        throw new UnsupportedOperationException("This configuration instance is immutable");
    }

    @Override
    public void addResource(URL url) {
        throw new UnsupportedOperationException("This configuration instance is immutable");
    }

    @Override
    public void addResource(Path file) {
        throw new UnsupportedOperationException("This configuration instance is immutable");
    }

    @Override
    public void addResource(InputStream in) {
        throw new UnsupportedOperationException("This configuration instance is immutable");
    }

    @Override
    public void reloadConfiguration() {
        //throw new UnsupportedOperationException("This configuration instance is immutable");
        encapsulated.reloadConfiguration(); // allowed to simplify testing
    }

    @Override
    public String get(String name) {
        return encapsulated.get(name);
    }

    @Override
    public String getRaw(String name) {
        return encapsulated.getRaw(name);
    }

    @Override
    public void set(String name, String value) {
        throw new UnsupportedOperationException("This configuration instance is immutable");
    }

    @Override
    public void unset(String name) {
        throw new UnsupportedOperationException("This configuration instance is immutable");
    }

    @Override
    public void setIfUnset(String name, String value) {
        throw new UnsupportedOperationException("This configuration instance is immutable");
    }

    @Override
    public String get(String name, String defaultValue) {
        return encapsulated.get(name, defaultValue);
    }

    @Override
    public int getInt(String name, int defaultValue) {
        return encapsulated.getInt(name, defaultValue);
    }

    @Override
    public void setInt(String name, int value) {
        throw new UnsupportedOperationException("This configuration instance is immutable");
    }

    @Override
    public long getLong(String name, long defaultValue) {
        return encapsulated.getLong(name, defaultValue);
    }

    @Override
    public void setLong(String name, long value) {
        throw new UnsupportedOperationException("This configuration instance is immutable");
    }

    @Override
    public float getFloat(String name, float defaultValue) {
        return encapsulated.getFloat(name, defaultValue);
    }

    @Override
    public void setFloat(String name, float value) {
        throw new UnsupportedOperationException("This configuration instance is immutable");
    }

    @Override
    public boolean getBoolean(String name, boolean defaultValue) {
        return encapsulated.getBoolean(name, defaultValue);
    }

    @Override
    public void setBoolean(String name, boolean value) {
        throw new UnsupportedOperationException("This configuration instance is immutable");
    }

    @Override
    public void setBooleanIfUnset(String name, boolean value) {
        throw new UnsupportedOperationException("This configuration instance is immutable");
    }

    @Override
    public <T extends Enum<T>> void setEnum(String name, T value) {
        throw new UnsupportedOperationException("This configuration instance is immutable");
    }

    @Override
    public <T extends Enum<T>> T getEnum(String name, T defaultValue) {
        return encapsulated.getEnum(name, defaultValue);
    }

    @Override
    public IntegerRanges getRange(String name, String defaultValue) {
        return encapsulated.getRange(name, defaultValue);
    }

    @Override
    public Collection<String> getStringCollection(String name) {
        return encapsulated.getStringCollection(name);
    }

    @Override
    public String[] getStrings(String name) {
        return encapsulated.getStrings(name);
    }

    @Override
    public String[] getStrings(String name, String... defaultValue) {
        return encapsulated.getStrings(name, defaultValue);
    }

    @Override
    public void setStrings(String name, String... values) {
        throw new UnsupportedOperationException("This configuration instance is immutable");
    }

    @Override
    public Class<?> getClassByName(String name) throws ClassNotFoundException {
        return encapsulated.getClassByName(name);
    }

    @Override
    public Class<?>[] getClasses(String name, Class<?>... defaultValue) {
        return encapsulated.getClasses(name, defaultValue);
    }

    @Override
    public Class<?> getClass(String name, Class<?> defaultValue) {
        return encapsulated.getClass(name, defaultValue);
    }

    @Override
    public <U> Class<? extends U> getClass(String name, Class<? extends U> defaultValue, Class<U> xface) {
        return encapsulated.getClass(name, defaultValue, xface);
    }

    @Override
    public <U> List<U> getInstances(String name, Class<U> xface) {
        return encapsulated.getInstances(name, xface);
    }

    @Override
    public void setClass(String name, Class<?> theClass, Class<?> xface) {
        throw new UnsupportedOperationException("This configuration instance is immutable");
    }

    @Override
    public Path getLocalPath(String dirsProp, String path) throws IOException {
        return encapsulated.getLocalPath(dirsProp, path);
    }

    @Override
    public File getFile(String dirsProp, String path) throws IOException {
        return encapsulated.getFile(dirsProp, path);
    }

    @Override
    public URL getResource(String name) {
        return encapsulated.getResource(name);
    }

    @Override
    public InputStream getConfResourceAsInputStream(String name) {
        return encapsulated.getConfResourceAsInputStream(name);
    }

    @Override
    public Reader getConfResourceAsReader(String name) {
        return encapsulated.getConfResourceAsReader(name);
    }

    @Override
    public int size() {
        return encapsulated.size();
    }

    @Override
    public void clear() {
        throw new UnsupportedOperationException("This configuration instance is immutable");
    }

    @Override
    public Iterator<Map.Entry<String, String>> iterator() {
        return encapsulated.iterator();
    }

    @Override
    public void writeXml(OutputStream out) throws IOException {
        encapsulated.writeXml(out);
    }

    @Override
    public void writeXml(Writer out) throws IOException {
        encapsulated.writeXml(out);
    }

    public static void dumpConfiguration(Configuration config, Writer out) throws IOException {
        Configuration.dumpConfiguration(config, out);
    }

    @Override
    public ClassLoader getClassLoader() {
        return encapsulated.getClassLoader();
    }

    @Override
    public void setClassLoader(ClassLoader classLoader) {
        throw new UnsupportedOperationException("This configuration instance is immutable");
    }

    @Override
    public String toString() {
        return encapsulated.toString();
    }

    @Override
    public void setQuietMode(boolean quietmode) {
        throw new UnsupportedOperationException("This configuration instance is immutable");
    }

    public static void main(String[] args) throws Exception {
        Configuration.main(args);
    }

    @Override
    public void readFields(DataInput in) throws IOException {
        encapsulated.readFields(in);
    }

    @Override
    public void write(DataOutput out) throws IOException {
        encapsulated.write(out);
    }

    @Override
    public Map<String, String> getValByRegex(String regex) {
        return encapsulated.getValByRegex(regex);
    }
}
