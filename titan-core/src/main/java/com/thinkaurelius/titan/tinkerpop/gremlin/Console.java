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

import java.io.*;
import java.nio.charset.Charset;

/**
 * The Gremlin console adapted for the Titan Graph Database
 *
 * @author Marko A. Rodriguez (http://markorodriguez.com)
 * @author Stephen Mallette (http://stephen.genoprime.com)
 */
public class Console {

    private static final String HISTORY_FILE = ".gremlin_titan_history";
    private static final String STANDARD_INPUT_PROMPT = "gremlin> ";
    private static final String STANDARD_RESULT_PROMPT = "==>";

    public Console(final IO io, final String inputPrompt, final String resultPrompt, final String initScriptFile) {
        io.out.println();
        io.out.println("         \\,,,/");
        io.out.println("         (o o)");
        io.out.println("-----oOOo-(_)-oOOo-----");

        final Groovysh groovy = new Groovysh();
        groovy.setResultHook(new NullResultHookClosure(groovy));

        // Gremlin + Titan imports
        for (String i : ConsoleSetup.getAllImportStatements()) {
            groovy.execute(i);
        }

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
        initializeShellWithScript(io, initScriptFile, groovy);

        try {
            runner.run();
        } catch (Error e) {
            //System.err.println(e.getMessage());
        }
    }

    public Console() {
        // opted to keep this constructor after adding the one with the initialization script file in case something
        // is using it...not sure if anything is.  if not, it can go.
        this(null);
    }

    public Console(final String initScriptFile) {
        this(new IO(System.in, System.out, System.err), STANDARD_INPUT_PROMPT, STANDARD_RESULT_PROMPT, initScriptFile);
    }

    private void initializeShellWithScript(final IO io, final String initScriptFile, final Groovysh groovy) {
        if (initScriptFile != null) {
            String line = "";
            try {
                final BufferedReader reader = new BufferedReader(new InputStreamReader(
                        new FileInputStream(initScriptFile), Charset.forName("UTF-8")));
                while ((line = reader.readLine()) != null) {
                    groovy.execute(line);
                }

                reader.close();
            } catch (FileNotFoundException fnfe) {
                io.err.println(String.format("Gremlin initialization file not found at [%s].", initScriptFile));
                System.exit(1);
            } catch (IOException ioe) {
                io.err.println(String.format("Bad line in Gremlin initialization file at [%s].", line));
                System.exit(1);
            }
        }
    }


    public static void main(final String[] args) {
        new Console(args.length == 1 ? args[0] : null);
    }
}
