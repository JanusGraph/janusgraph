package com.thinkaurelius.titan.tinkerpop.gremlin;

import java.io.File;
import java.io.FileNotFoundException;
import java.io.PrintStream;
import java.util.List;

import com.google.common.base.Joiner;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.Lists;


/**
 * Collect and print statements evaluated when
 * setting up a Titan + Gremlin-Groovy console session.
 */
public class ConsoleSetup {
    
    private static final ImmutableList<String> imports;
    private static final ImmutableList<String> staticImports;
    private static final ImmutableList<String> allImportsGremlin;
    
    static {
        ImmutableList.Builder<String> si = ImmutableList.builder();
        ImmutableList.Builder<String>  i = ImmutableList.builder();
        
        // Tinkerpop imports (Gremlin, Blueprints, Pipes, ...)
        for (String s : com.tinkerpop.gremlin.Imports.getImports()) {
            ImmutableList.Builder<String> b;
            
            if (s.startsWith("static ")) {
                s = s.substring(7);
                b = si;
            } else {
                b = i;
            }
            
            b.add(s);
        }
        
        // Titan imports
        for (String s : Imports.getImports()) {
            ImmutableList.Builder<String> b;
            
            if (s.startsWith("static ")) {
                s = s.substring(7);
                b = si;
            } else {
                b = i;
            }
            
            b.add(s);
        }
        
        // "necessary due to Gremlin Groovy" (I don't know what that means)
        i.add("com.tinkerpop.gremlin.Tokens.T");
        i.add("com.tinkerpop.gremlin.groovy.*");
        
        imports = i.build();
        staticImports = si.build();
        
        ImmutableList.Builder<String> allBuilder = ImmutableList.builder();
        for (String s : imports)
            allBuilder.add("import " + s + ";");
        for (String s : staticImports)
            allBuilder.add("import static " + s + ";");
        
        allImportsGremlin = allBuilder.build();
    }

    public static List<String> getNonStaticImports() {
        return imports;
    }
    
    public static List<String> getStaticImports() {
        return staticImports;
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
    public static List<String> getAllImportStatements() {
        return allImportsGremlin;
    }
    
    /**
     * Write a properties file with two properties: imports and staticimports.
     * These are set to comma-separated lists of {@link #getNonStaticImports()}
     * and {@link #getStaticImports()}, respectively.
     * 
     * @param args
     *            the file to write (stdout if unspecified)
     * @throws FileNotFoundException
     *             shouldn't happen since we only open the target file for
     *             overwriting and never for appending
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
        
        Joiner commas = Joiner.on(',');
        
        s.print("imports = ");
        s.println(commas.join(getNonStaticImports()));
        
        s.print("staticimports = ");
        s.println(commas.join(getStaticImports()));
        
        s.close();
    }
}
