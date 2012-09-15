package com.thinkaurelius.faunus.tinkerpop.gremlin;

import com.thinkaurelius.faunus.FaunusPipeline;
import com.tinkerpop.gremlin.groovy.console.ArrayIterator;
import com.tinkerpop.pipes.util.iterators.SingleIterator;
import groovy.lang.Closure;
import org.codehaus.groovy.tools.shell.IO;

import java.util.Collections;
import java.util.Iterator;
import java.util.Map;

/**
 * @author Marko A. Rodriguez (http://markorodriguez.com)
 */
public class ResultHookClosure extends Closure {
    private final String resultPrompt;
    private final IO io;

    public ResultHookClosure(final Object owner, final IO io, final String resultPrompt) {
        super(owner);
        this.io = io;
        this.resultPrompt = resultPrompt;
    }

    public Object call(final Object[] args) {
        final Object result = args[0];
        final Iterator itty;
        if (result instanceof FaunusPipeline) {
            try {
                ((FaunusPipeline) result).submit();
                itty = Collections.emptyList().iterator();
            } catch (Exception e) {
                throw new RuntimeException(e.getMessage(), e);
            }
        } else if (result instanceof Iterator) {
            itty = (Iterator) result;
        } else if (result instanceof Iterable) {
            itty = ((Iterable) result).iterator();
        } else if (result instanceof Object[]) {
            itty = new ArrayIterator((Object[]) result);
        } else if (result instanceof Map) {
            itty = ((Map) result).entrySet().iterator();
        } else {
            itty = new SingleIterator<Object>(result);
        }

        while (itty.hasNext()) {
            this.io.out.println(this.resultPrompt + itty.next());
        }

        return null;
    }
}