// Copyright 2017 JanusGraph Authors
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//      http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package org.janusgraph.hadoop;

import org.apache.hadoop.classification.InterfaceStability;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.File;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.io.Reader;
import java.io.Writer;
import java.net.InetSocketAddress;
import java.net.URL;
import java.util.Collection;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.concurrent.TimeUnit;
import java.util.regex.Pattern;

public class ImmutableConfiguration extends Configuration {

    private final Configuration encapsulated;

    public ImmutableConfiguration(Configuration encapsulated) {
        this.encapsulated = encapsulated;
    }

    @Deprecated
    public static void addDeprecation(String key, String[] newKeys, String customMessage) {
        Configuration.addDeprecation(key, newKeys, customMessage);
    }

    public static void addDeprecation(String key, String newKey, String customMessage) {
        Configuration.addDeprecation(key, newKey, customMessage);
    }

    @Deprecated
    public static void addDeprecation(String key, String[] newKeys) {
        Configuration.addDeprecation(key, newKeys);
    }

    public static void addDeprecation(String key, String newKey) {
        Configuration.addDeprecation(key, newKey);
    }

    public static boolean isDeprecated(String key) {
        return Configuration.isDeprecated(key);
    }

    public static void addDefaultResource(String name) {
        Configuration.addDefaultResource(name);
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
    public void addResource(InputStream in, String name) {
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
    public String getTrimmed(String name) {
        return encapsulated.getTrimmed(name);
    }

    @Override
    public String getTrimmed(String name, String defaultValue) {
        return encapsulated.getTrimmed(name, defaultValue);
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
    public void set(String name, String value, String source) {
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
    public int[] getInts(String name) {
        return encapsulated.getInts(name);
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
    public long getLongBytes(String name, long defaultValue) {
        return encapsulated.getLongBytes(name, defaultValue);
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
    public double getDouble(String name, double defaultValue) {
        return encapsulated.getDouble(name, defaultValue);
    }

    @Override
    public void setDouble(String name, double value) {
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
    public void setTimeDuration(String name, long value, TimeUnit unit) {
        throw new UnsupportedOperationException("This configuration instance is immutable");
    }

    @Override
    public long getTimeDuration(String name, long defaultValue, TimeUnit unit) {
        return encapsulated.getTimeDuration(name, defaultValue, unit);
    }

    @Override
    public Pattern getPattern(String name, Pattern defaultValue) {
        return encapsulated.getPattern(name, defaultValue);
    }

    @Override
    public void setPattern(String name, Pattern pattern) {
        throw new UnsupportedOperationException("This configuration instance is immutable");
    }

    @Override
    @InterfaceStability.Unstable
    public String[] getPropertySources(String name) {
        return encapsulated.getPropertySources(name);
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
    public Collection<String> getTrimmedStringCollection(String name) {
        return encapsulated.getTrimmedStringCollection(name);
    }

    @Override
    public String[] getTrimmedStrings(String name) {
        return encapsulated.getTrimmedStrings(name);
    }

    @Override
    public String[] getTrimmedStrings(String name, String... defaultValue) {
        return encapsulated.getTrimmedStrings(name, defaultValue);
    }

    @Override
    public void setStrings(String name, String... values) {
        throw new UnsupportedOperationException("This configuration instance is immutable");
    }

    @Override
    public InetSocketAddress getSocketAddr(String name, String defaultAddress, int defaultPort) {
        return encapsulated.getSocketAddr(name, defaultAddress, defaultPort);
    }

    @Override
    public void setSocketAddr(String name, InetSocketAddress address) {
        throw new UnsupportedOperationException("This configuration instance is immutable");
    }

    @Override
    public InetSocketAddress updateConnectAddr(String name, InetSocketAddress address) {
        throw new UnsupportedOperationException("This configuration instance is immutable");
    }

    @Override
    public Class<?> getClassByName(String name) throws ClassNotFoundException {
        return encapsulated.getClassByName(name);
    }

    @Override
    public Class<?> getClassByNameOrNull(String name) {
        return encapsulated.getClassByNameOrNull(name);
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

    /*
     * The encapsulated method impl creates a new HashMap for each invocation and returns an
     * iterator on that newly-created HashMap.  So, remove is allowed but doesn't mutate the
     * state of the Configuration.  This is not documented and might change.  It also might
     * be safer to throw an exception on remove since it won't have the effect the client code
     * probably intends.
     */
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
    public void setQuietMode(boolean quietMode) {
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

    public static void dumpDeprecatedKeys() {
        Configuration.dumpDeprecatedKeys();
    }
}
