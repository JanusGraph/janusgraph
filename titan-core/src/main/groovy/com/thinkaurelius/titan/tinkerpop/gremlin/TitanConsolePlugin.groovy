package com.thinkaurelius.titan.tinkerpop.gremlin

import com.thinkaurelius.titan.core.attribute.Geo
import com.thinkaurelius.titan.core.attribute.Text
import com.tinkerpop.gremlin.groovy.console.ConsoleGroovy
import com.tinkerpop.gremlin.groovy.console.ConsoleIO
import com.tinkerpop.gremlin.groovy.console.ConsolePlugin

/**
 * @author Stephen Mallette (http://stephen.genoprime.com)
 */
class TitanConsolePlugin implements ConsolePlugin {
    String getName() {
        return "titan"
    }

    void pluginTo(ConsoleGroovy consoleGroovy, ConsoleIO consoleIO, Map map) {
        consoleGroovy.execute("import com.thinkaurelius.titan.core.*")
        consoleGroovy.execute("import com.thinkaurelius.titan.core.attribute.*");
        consoleGroovy.execute("import com.thinkaurelius.titan.core.util.*");
        consoleGroovy.execute("import com.thinkaurelius.titan.example.*");
        consoleGroovy.execute("import org.apache.commons.configuration.*");
        consoleGroovy.execute("import static " + Geo.class.getName() + ".*");
        consoleGroovy.execute("import static " + Text.class.getName() + ".*");
    }
}
