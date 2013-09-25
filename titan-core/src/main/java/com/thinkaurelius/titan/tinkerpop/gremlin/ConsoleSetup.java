package com.thinkaurelius.titan.tinkerpop.gremlin;

import java.io.File;
import java.io.FileNotFoundException;
import java.io.PrintStream;
import java.util.List;

import com.google.common.collect.ImmutableList;


/**
 * Collect and print statements evaluated when
 * setting up a Titan + Gremlin-Groovy console session.
 */
public class ConsoleSetup {
    
    private static final ImmutableList<String> setupScript;
    
    static {
        ImmutableList.Builder<String> builder = ImmutableList.builder();
        
        // Tinkerpop imports (Gremlin, Blueprints, Pipes, ...)
        for (String s : com.tinkerpop.gremlin.Imports.getImports())
            builder.add("import " + s + ";");
        
        // Titan imports
        for (String s : Imports.getImports())
            builder.add("import " + s + ";");
        
        // "necessary due to Gremlin Groovy" (I don't know what that means)
        builder.add("import com.tinkerpop.gremlin.Tokens.T;");
        builder.add("import com.tinkerpop.gremlin.groovy.*;");
        
        setupScript = builder.build();
    }
    
    /**
     * Return a sequence of import statements to prepare a just-instantiated
     * Gremlin-Groovy interpreter. This includes both Tinkerpop stack and Titan
     * symbol imports.
     * 
     * Though intended for use in Gremlin-Groovy, the returned import statements
     * all end with semicolons, so the statements are valid under the syntax of
     * both Groovy and Java.
     * 
     * This only returns import and import static statements (at least, as of
     * the last time this comment was updated).
     * 
     * @return A sequence of import statements
     */
    public static List<String> getAllImports() {
        return setupScript;
    }
    
    /**
     * Call {@link #getAllImports()} and print the result to stdout or a file
     * 
     * @param args
     *            if present, the first element is is the output file path; if
     *            empty, then use stdout instead
     * @throws FileNotFoundException
     *             this should never happen because we open the target file for
     *             overwriting and not for appending
     */
    public static void main(String args[]) throws FileNotFoundException {
        
        PrintStream s = System.out;
        
        if (0 < args.length) {
            File f = new File(args[0]);
            
            File parent = f.getParentFile();
            
            if (null != parent && !parent.exists())
                parent.mkdirs();
            
            s = new PrintStream(f);
        }
        
        for (String i : getAllImports())
            s.println(i);
        
        s.close();
    }
}
