package com.thinkaurelius.titan.tinkerpop.gremlin;

import com.tinkerpop.gremlin.groovy.Gremlin;
import com.tinkerpop.gremlin.groovy.console.ErrorHookClosure;
import com.tinkerpop.gremlin.groovy.console.NullResultHookClosure;
import com.tinkerpop.gremlin.groovy.console.PromptClosure;
import com.tinkerpop.gremlin.groovy.console.ResultHookClosure;
import jline.History;
import org.codehaus.groovy.tools.shell.Groovysh;
import org.codehaus.groovy.tools.shell.IO;
import org.codehaus.groovy.tools.shell.InteractiveShellRunner;

import java.io.File;
import java.io.IOException;

/**
 * @author Marko A. Rodriguez (http://markorodriguez.com)
 */
public class Console {

    private static final String HISTORY_FILE = ".gremlin_titan_history";
    private static final String STANDARD_INPUT_PROMPT = "gremlin> ";
    private static final String STANDARD_RESULT_PROMPT = "==>";

    public Console(final IO io, final String inputPrompt, final String resultPrompt) {
        io.out.println();
        io.out.println("         \\,,,/");
        io.out.println("         (o o)");
        io.out.println("-----oOOo-(_)-oOOo-----");

        final Groovysh groovy = new Groovysh();
        groovy.setResultHook(new NullResultHookClosure(groovy));
        for (String imps : Imports.getImports()) {
            groovy.execute("import " + imps);
        }
        groovy.execute("import com.tinkerpop.gremlin.Tokens.T");
        groovy.execute("import com.tinkerpop.gremlin.groovy.*");

        groovy.setResultHook(new ResultHookClosure(groovy, io, resultPrompt));
        groovy.setHistory(new History());

        final InteractiveShellRunner runner = new InteractiveShellRunner(groovy, new PromptClosure(groovy, inputPrompt));
        runner.setErrorHandler(new ErrorHookClosure(runner, io));
        try {
            runner.setHistory(new History(new File(System.getProperty("user.home") + "/" + HISTORY_FILE)));
        } catch (IOException e) {
            io.err.println("Unable to create history file: " + HISTORY_FILE);
        }

        Gremlin.load();
        try {
            runner.run();
        } catch (Error e) {
            //System.err.println(e.getMessage());
        }
    }

    public Console() {
        this(new IO(System.in, System.out, System.err), STANDARD_INPUT_PROMPT, STANDARD_RESULT_PROMPT);
    }


    public static void main(final String[] args) {
        new Console();
    }
}
