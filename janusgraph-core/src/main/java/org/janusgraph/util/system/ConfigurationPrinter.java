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

package org.janusgraph.util.system;

import java.io.FileOutputStream;
import java.io.IOException;
import java.io.PrintStream;
import java.time.Duration;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.List;
import com.google.common.base.Function;
import com.google.common.base.Joiner;
import com.google.common.collect.Iterables;

import org.janusgraph.core.util.ReflectiveConfigOptionLoader;
import org.janusgraph.diskstorage.configuration.ConfigElement;
import org.janusgraph.diskstorage.configuration.ConfigNamespace;
import org.janusgraph.diskstorage.configuration.ConfigOption;

import static org.janusgraph.util.encoding.StringEncoding.UTF8_ENCODING;

/**
 * Recursively dump the root configuration namespace to either System.out or the
 * named file in a CSV-type format with configurable delimiter, header, and
 * footer. Used to generate a table of all configuration keys for inclusion in
 * the AsciiDoc documentation.
 */
public class ConfigurationPrinter {

//    private static final String TABLE_HEADER_LINES = "[role=\"tss-config-table\",cols=\"2,3,1,1,1\",options=\"header\",width=\"100%\"]\n|=====\n| Name | Description | Datatype | Default Value | Mutability";
    private static final String DELIM = "|";
    private static final String DELIM_PADDING = " ";
    private static final String TABLE_FOOTER_LINES = "|=====\n";
    private static final boolean DELIM_AT_LINE_START = true;
    private static final boolean DELIM_AT_LINE_END = false;

    private final boolean showMutability;
    private final PrintStream stream;

    public static void main(String args[]) throws IOException, ReflectiveOperationException {

        ReflectiveConfigOptionLoader.INSTANCE.loadStandard(ConfigurationPrinter.class);

        // Write to filename argument
        if (3 != args.length) {
            System.err.println("Usage: " + ConfigurationPrinter.class.getName() +
                    " <package.class.fieldname of a ConfigNamespace root> <output filename> <display mutabilities>");
            System.exit(-1);
        }

        final ConfigNamespace root = stringToNamespace(args[0]);
        final PrintStream stream = new PrintStream(new FileOutputStream(args[1]), false, UTF8_ENCODING);
        final boolean mutability = Boolean.valueOf(args[2]);

        new ConfigurationPrinter(stream, mutability).write(root);

        stream.flush();
        stream.close();
    }

    private ConfigurationPrinter(PrintStream stream, boolean showMutability) {
        this.stream = stream;
        this.showMutability = showMutability;
    }

    private void write(ConfigNamespace root) {
        printNamespace(root, "");
    }

    private static ConfigNamespace stringToNamespace(String raw)
            throws ClassNotFoundException, NoSuchFieldException, IllegalAccessException {
        int i = raw.lastIndexOf(".");

        String fullClassName = raw.substring(0, i);
        String fieldName = raw.substring(i + 1);

        return (ConfigNamespace)Class.forName(fullClassName).getField(fieldName).get(null);
    }

    private String getFullPath(ConfigOption<?> opt) {
        StringBuilder sb = new StringBuilder(64);
        sb.insert(0, opt.getName());
        for (ConfigNamespace parent = opt.getNamespace();
             null != parent && !parent.isRoot();
             parent = parent.getNamespace()) {
            if (parent.isUmbrella())
                sb.insert(0, "[X].");
            sb.insert(0, ".");
            sb.insert(0, parent.getName());
        }
        return sb.toString();
    }

    private void printNamespace(ConfigNamespace n, String prefix) {
        String newPrefix = prefix + n.getName() + ".";
        if (n.isUmbrella()) newPrefix += "[X].";

        if (n.isRoot()) newPrefix = ""; //Override for root namespace

        //Only print namespace if it contains (visible) options
        if (!Iterables.isEmpty(getSortedChildOptions(n))) {
            stream.println(getNamespaceSectionHeader(n, prefix));
            stream.println(getTableHeader());
            for (ConfigOption<?> o : getSortedChildOptions(n)) {
                stream.println(getTableLineForOption(o, newPrefix));
            }
            stream.println(TABLE_FOOTER_LINES);
        }

        for (ConfigNamespace cn : getSortedChildNamespaces(n)) {
            printNamespace(cn, newPrefix);
        }
    }

    private String getNamespaceSectionHeader(ConfigNamespace n, String prefix) {
        String fullName = prefix + n.getName();
        if (n.isUmbrella()) fullName += " *";
        return "==== " + fullName + " ====\n[role=\"font16\"]\n" + n.getDescription() + "\n\n";
    }

    private List<ConfigOption<?>> getSortedChildOptions(ConfigNamespace n) {
        return getSortedChildren(n, arg0 -> arg0.isOption() && !((ConfigOption)arg0).isHidden());
    }

    private List<ConfigNamespace> getSortedChildNamespaces(ConfigNamespace n) {
        return getSortedChildren(n, ConfigElement::isNamespace);
    }

    private String getTableLineForOption(ConfigOption o, String prefix) {

        final List<String> colData = new ArrayList<>(10);
        String name = prefix + o.getName();
        if (o.isDeprecated()) {
            ConfigOption<?> successor = o.getDeprecationReplacement();
            if (null == successor) {
                name = "[deprecation-warning]*Deprecated* [line-through]#" + name + "#";
            } else {
                name = "[deprecation-warning]*Deprecated.  See " + getFullPath(successor) + "* [line-through]#" + name + "#";
            }

        }
        colData.add(name);
        colData.add(removeDelim(o.getDescription()));
        colData.add(o.getDatatype().getSimpleName());
        colData.add(removeDelim(getStringForDefaultValue(o)));

        if (showMutability)
            colData.add(o.getType().toString());

        String line = Joiner.on(DELIM_PADDING + DELIM + DELIM_PADDING).join(colData);

        if (DELIM_AT_LINE_START) {
            line = DELIM + DELIM_PADDING + line;
        }

        if (DELIM_AT_LINE_END) {
            line = line + DELIM_PADDING + DELIM;
        }

        return line;
    }

    private String getTableHeader() {
        String colWidths = "2,3,1,1"; // Name, Desc, Datatype, Default
        String colNames = "Name | Description | Datatype | Default Value";
        if (showMutability) {
            colWidths += ",1"; // Mutability level
            colNames  += " | Mutability";
        }
        return "[role=\"tss-config-table\",cols=\"" + colWidths + "\",options=\"header\",width=\"100%\"]\n" +
                "|=====\n" +
                "| " + colNames; // no terminal newline required
    }

    @SuppressWarnings("unchecked")
    private <E> List<E> getSortedChildren(ConfigNamespace n, Function<ConfigElement, Boolean> predicate) {
        final List<ConfigElement> sortedElements = new ArrayList<>();

        for (ConfigElement e : n.getChildren()) {
            if (predicate.apply(e)) {
                sortedElements.add(e);
            }
        }
        sortedElements.sort(Comparator.comparing(ConfigElement::getName));

        return (List<E>)sortedElements;
    }

    private String removeDelim(String s) {
        return s.replace(DELIM, "");
    }

    private String getStringForDefaultValue(ConfigOption<?> c) {
        Object o = c.getDefaultValue();

        if (null == o) {
            return "(no default value)";
        } else if (o instanceof Duration) {
            Duration d = (Duration)o;
            return d.toMillis() + " ms";
        } else if (o instanceof String[]) {
            return Joiner.on(",").join((String[])o);
        }

        return o.toString();
    }
}
