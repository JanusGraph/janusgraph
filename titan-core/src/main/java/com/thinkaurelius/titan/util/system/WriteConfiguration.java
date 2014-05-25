package com.thinkaurelius.titan.util.system;

import java.io.File;
import java.io.FileNotFoundException;
import java.io.PrintStream;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Comparator;
import java.util.List;
import java.util.concurrent.TimeUnit;

import com.google.common.base.Joiner;
import com.thinkaurelius.titan.core.attribute.Duration;
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
public class WriteConfiguration {

    private static final String HEADER = "[cols=\"2,3,1,1,1\",options=\"header\"]\n|=====\n| Name | Description | Datatype | Default Value | Mutability";
    private static final String DELIM = "|";
    private static final String DELIM_PADDING = " ";
    private static final String FOOTER = "|=====";
    private static boolean DELIM_AT_START = true;
    private static boolean DELIM_AT_END = false;

    private final PrintStream stream;

    public static void main(String args[]) throws FileNotFoundException {

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

        new WriteConfiguration(stream).write(GraphDatabaseConfiguration.ROOT_NS);

        stream.flush();
        stream.close();
    }

    private WriteConfiguration(PrintStream stream) {
        this.stream = stream;
    }

    private void write(ConfigNamespace root) {
        stream.println(HEADER);
        printNamespace(root, "");
        stream.println(FOOTER);
    }

    private void printNamespace(ConfigNamespace n, String prefix) {

        List<ConfigElement> sortedElements = new ArrayList<ConfigElement>();
        for (ConfigElement e : n.getChildren()) {
            sortedElements.add(e);
        }
        Collections.sort(sortedElements, new Comparator<ConfigElement>() {
            @Override
            public int compare(ConfigElement o1, ConfigElement o2) {
                return o1.getName().compareTo(o2.getName());
            }
        });
        for (ConfigElement e : sortedElements) {
            if (e.isNamespace()) {
                ConfigNamespace next = (ConfigNamespace)e;
                final String newPrefix = prefix + next.getName() + ".";
                printNamespace(next, newPrefix);
            } else if (e.isOption()) {
                ConfigOption<?> o = (ConfigOption<?>)e;

                String line = Joiner.on(DELIM_PADDING + DELIM + DELIM_PADDING).join(
                        prefix + o.getName(),
                        removeDelim(o.getDescription()),
                        o.getDatatype().getSimpleName(),
                        removeDelim(getStringForDefaultValue(o)),
                        o.getType());

                if (DELIM_AT_START) {
                    line = DELIM + DELIM_PADDING + line;
                }

                if (DELIM_AT_END) {
                    line = line + DELIM_PADDING + DELIM;
                }

                stream.println(line);
            }
        }
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
