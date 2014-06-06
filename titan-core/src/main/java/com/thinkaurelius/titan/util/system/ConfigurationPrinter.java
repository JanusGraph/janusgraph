package com.thinkaurelius.titan.util.system;

import java.io.File;
import java.io.FileNotFoundException;
import java.io.PrintStream;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Comparator;
import java.util.List;
import java.util.concurrent.TimeUnit;

import com.google.common.base.Function;
import com.google.common.base.Joiner;
import com.thinkaurelius.titan.core.attribute.Duration;
import com.thinkaurelius.titan.core.util.ReflectiveConfigOptionLoader;
import com.thinkaurelius.titan.diskstorage.configuration.ConfigElement;
import com.thinkaurelius.titan.diskstorage.configuration.ConfigNamespace;
import com.thinkaurelius.titan.diskstorage.configuration.ConfigOption;
import com.thinkaurelius.titan.graphdb.configuration.GraphDatabaseConfiguration;

/**
 * Recursively dump the root configuration namespace to either System.out or the
 * named file in a CSV-type format with configurable delimiter, header, and
 * footer. Used to generate a table of all configuration keys for inclusion in
 * the AsciiDoc documentation.
 */
public class ConfigurationPrinter {
    private static final String TABLE_HEADER_LINES = "[role=\"tss-config-table\",cols=\"2,3,1,1,1\",options=\"header\",width=\"100%\"]\n|=====\n| Name | Description | Datatype | Default Value | Mutability";
    private static final String DELIM = "|";
    private static final String DELIM_PADDING = " ";
    private static final String TABLE_FOOTER_LINES = "|=====\n";
    private static boolean DELIM_AT_LINE_START = true;
    private static boolean DELIM_AT_LINE_END = false;

    private final PrintStream stream;

    public static void main(String args[]) throws FileNotFoundException {

        ReflectiveConfigOptionLoader.loadOnce();

        final PrintStream stream;
        if (args.length == 1) {
            File f = new File(args[0]);
            File dir = f.getParentFile();
            if (!dir.exists()) {
                dir.mkdirs();
            }
            stream = new PrintStream(f);
        } else {
            stream = System.out;
        }

        new ConfigurationPrinter(stream).write(GraphDatabaseConfiguration.ROOT_NS);

        stream.flush();
        stream.close();
    }

    private ConfigurationPrinter(PrintStream stream) {
        this.stream = stream;
    }

    private void write(ConfigNamespace root) {
        printNamespace(root, "");
    }

    private void printNamespace(ConfigNamespace n, String prefix) {

        stream.println(getNamespaceSectionHeader(n));
        stream.println(TABLE_HEADER_LINES);
        for (ConfigOption<?> o : getSortedChildOptions(n)) {
            stream.println(getTableLineForOption(o, prefix));
        }
        stream.println(TABLE_FOOTER_LINES);

        for (ConfigNamespace cn : getSortedChildNamespaces(n)) {
            final String newPrefix = prefix + cn.getName() + ".";
            printNamespace(cn, newPrefix);
        }
    }

    private String getNamespaceSectionHeader(ConfigNamespace n) {
        String fullName = ConfigElement.getPath(n);
        return "==== " + fullName + " ====\n[role=\"font16\"]\n" + n.getDescription() + "\n\n";
    }

    private List<ConfigOption<?>> getSortedChildOptions(ConfigNamespace n) {
        return getSortedChildren(n, new Function<ConfigElement, Boolean>() {
            @Override
            public Boolean apply(ConfigElement arg0) {
                return arg0.isOption();
            }
        });
    }

    private List<ConfigNamespace> getSortedChildNamespaces(ConfigNamespace n) {
        return getSortedChildren(n, new Function<ConfigElement, Boolean>() {
            @Override
            public Boolean apply(ConfigElement arg0) {
                return arg0.isNamespace();
            }
        });
    }

        private String getTableLineForOption(ConfigOption o, String prefix) {

            String line = Joiner.on(DELIM_PADDING + DELIM + DELIM_PADDING).join(
                    prefix + o.getName(),
                    removeDelim(o.getDescription()),
                    o.getDatatype().getSimpleName(),
                    removeDelim(getStringForDefaultValue(o)),
                    o.getType());

            if (DELIM_AT_LINE_START) {
                line = DELIM + DELIM_PADDING + line;
            }

            if (DELIM_AT_LINE_END) {
                line = line + DELIM_PADDING + DELIM;
            }

            return line;
        }

    @SuppressWarnings("unchecked")
    private <E> List<E> getSortedChildren(ConfigNamespace n, Function<ConfigElement, Boolean> predicate) {
        List<ConfigElement> sortedElements = new ArrayList<ConfigElement>();

        for (ConfigElement e : n.getChildren()) {
            if (predicate.apply(e)) {
                sortedElements.add(e);
            }
        }
        Collections.sort(sortedElements, new Comparator<ConfigElement>() {
            @Override
            public int compare(ConfigElement o1, ConfigElement o2) {
                return o1.getName().compareTo(o2.getName());
            }
        });

        return (List<E>)sortedElements;
    }

    private String removeDelim(String s) {
        return s.replace(DELIM, "");
    }

    private String getStringForDefaultValue(ConfigOption<?> c) {
        Object o = c.getDefaultValue();

        if (null == o) {
            return " ";
        } else if (o instanceof Duration) {
            Duration d = (Duration)o;
            return d.getLength(TimeUnit.MILLISECONDS) + " ms";
        } else if (o instanceof String[]) {
            return Joiner.on(",").join((String[])o);
        }

        return o.toString();
    }
}
