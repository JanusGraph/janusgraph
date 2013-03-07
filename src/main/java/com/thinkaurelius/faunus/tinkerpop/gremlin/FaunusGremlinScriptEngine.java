package com.thinkaurelius.faunus.tinkerpop.gremlin;

import com.tinkerpop.gremlin.groovy.Gremlin;
import com.tinkerpop.gremlin.groovy.jsr223.GremlinGroovyScriptEngine;
import com.tinkerpop.gremlin.groovy.jsr223.GremlinGroovyScriptEngineFactory;
import groovy.lang.Binding;
import groovy.lang.Closure;
import groovy.lang.DelegatingMetaClass;
import groovy.lang.GroovyClassLoader;
import groovy.lang.MetaClass;
import groovy.lang.MissingMethodException;
import groovy.lang.MissingPropertyException;
import groovy.lang.Script;
import groovy.lang.Tuple;
import org.codehaus.groovy.control.CompilationFailedException;
import org.codehaus.groovy.control.CompilerConfiguration;
import org.codehaus.groovy.control.customizers.ImportCustomizer;
import org.codehaus.groovy.jsr223.GroovyCompiledScript;
import org.codehaus.groovy.jsr223.GroovyScriptEngineImpl;
import org.codehaus.groovy.runtime.InvokerHelper;
import org.codehaus.groovy.runtime.MetaClassHelper;
import org.codehaus.groovy.runtime.MethodClosure;
import org.codehaus.groovy.syntax.SyntaxException;

import javax.script.Bindings;
import javax.script.CompiledScript;
import javax.script.ScriptContext;
import javax.script.ScriptEngineFactory;
import javax.script.ScriptException;
import javax.script.SimpleBindings;
import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.io.PrintWriter;
import java.io.Reader;
import java.lang.reflect.InvocationHandler;
import java.lang.reflect.Method;
import java.lang.reflect.Proxy;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

/**
 * @author Marko A. Rodriguez (http://markorodriguez.com)
 */
public class FaunusGremlinScriptEngine extends GroovyScriptEngineImpl {

    private Map<String, Class> classMap = new ConcurrentHashMap<String, Class>();
    private Map<String, MethodClosure> globalClosures = new ConcurrentHashMap<String, MethodClosure>();
    protected GroovyClassLoader loader;
    private volatile GremlinGroovyScriptEngineFactory factory;
    private static int counter = 0;
    private int cacheResetSize = 1500;

    private static final String DOT_STAR = ".*";
    private static final String EMPTY_STRING = "";

    public FaunusGremlinScriptEngine(final int cacheResetSize) {
        this();
        this.cacheResetSize = cacheResetSize;
    }

    public FaunusGremlinScriptEngine() {
        Gremlin.load();
        FaunusGremlin.load();
        final CompilerConfiguration conf = new CompilerConfiguration();
        conf.addCompilationCustomizers(getImportCustomizer());
        this.loader = new GroovyClassLoader(getParentLoader(), conf);
    }

    public static ImportCustomizer getImportCustomizer() {
        final ImportCustomizer ic = GremlinGroovyScriptEngine.getImportCustomizer();
        for (final String imp : Imports.getImports()) {
            ic.addStarImports(imp.replace(DOT_STAR, EMPTY_STRING));
        }
        return ic;
    }

    private void checkClearCache() {
        if (this.classMap.size() > this.cacheResetSize) {
            this.globalClosures.clear();
            this.classMap.clear();
            this.loader.clearCache();
        }
    }

    public Object eval(final Reader reader, final ScriptContext context) throws ScriptException {
        return eval(readFully(reader), context);
    }

    public Object eval(final String script, final ScriptContext context) throws ScriptException {
        try {
            return eval(getScriptClass(script), context);
        } catch (SyntaxException e) {
            throw new ScriptException(e.getMessage(), e.getSourceLocator(), e.getLine());
        } catch (Exception e) {
            throw new ScriptException(e);
        }
    }

    public Bindings createBindings() {
        return new SimpleBindings();
    }

    public ScriptEngineFactory getFactory() {
        if (this.factory == null) {
            synchronized (this) {
                if (this.factory == null) {
                    this.factory = new GremlinGroovyScriptEngineFactory();
                }
            }
        }
        return this.factory;
    }

    public CompiledScript compile(final String scriptSource) throws ScriptException {
        try {
            return new GroovyCompiledScript(this, getScriptClass(scriptSource));
        } catch (SyntaxException e) {
            throw new ScriptException(e.getMessage(), e.getSourceLocator(), e.getLine());
        } catch (IOException e) {
            throw new ScriptException(e);
        } catch (CompilationFailedException ee) {
            throw new ScriptException(ee);
        }
    }

    public CompiledScript compile(final Reader reader) throws ScriptException {
        return compile(readFully(reader));
    }

    public Object invokeFunction(final String name, final Object args[]) throws ScriptException, NoSuchMethodException {
        return invokeImpl(null, name, args);
    }

    public Object invokeMethod(final Object thiz, final String name, final Object args[]) throws ScriptException, NoSuchMethodException {
        if (thiz == null) {
            throw new IllegalArgumentException("Script object can not be null");
        } else {
            return invokeImpl(thiz, name, args);
        }
    }

    public Object getInterface(final Class clasz) {
        return makeInterface(null, clasz);
    }

    public Object getInterface(final Object thiz, final Class clasz) {
        if (thiz == null)
            throw new IllegalArgumentException("Script object can not be null");
        else
            return makeInterface(thiz, clasz);
    }

    Object eval(final Class scriptClass, final ScriptContext context) throws ScriptException {
        this.checkClearCache();

        context.setAttribute("context", context, ScriptContext.ENGINE_SCOPE);
        java.io.Writer writer = context.getWriter();
        context.setAttribute("out", writer instanceof PrintWriter ? writer : new PrintWriter(writer), ScriptContext.ENGINE_SCOPE);
        Binding binding = new Binding() {
            public Object getVariable(String name) {
                synchronized (context) {
                    int scope = context.getAttributesScope(name);
                    if (scope != -1) {
                        return context.getAttribute(name, scope);
                    }
                    throw new MissingPropertyException(name, getClass());
                }
            }

            public void setVariable(String name, Object value) {
                synchronized (context) {
                    int scope = context.getAttributesScope(name);
                    if (scope == -1) {
                        scope = ScriptContext.ENGINE_SCOPE;
                    }
                    context.setAttribute(name, value, scope);
                }
            }
        };
        try {
            Script scriptObject = InvokerHelper.createScript(scriptClass, binding);
            Method methods[] = scriptClass.getMethods();
            Map<String, MethodClosure> closures = new HashMap<String, MethodClosure>();
            for (Method m : methods) {
                String name = m.getName();
                closures.put(name, new MethodClosure(scriptObject, name));
            }

            globalClosures.putAll(closures);
            final MetaClass oldMetaClass = scriptObject.getMetaClass();
            scriptObject.setMetaClass(new DelegatingMetaClass(oldMetaClass) {
                public Object invokeMethod(Object object, String name, Object args) {
                    if (args == null) {
                        return invokeMethod(object, name, MetaClassHelper.EMPTY_ARRAY);
                    } else if (args instanceof Tuple) {
                        return invokeMethod(object, name, ((Tuple) args).toArray());
                    } else if (args instanceof Object[]) {
                        return invokeMethod(object, name, (Object[]) args);
                    } else {
                        return invokeMethod(object, name, new Object[]{args});
                    }
                }

                public Object invokeMethod(Object object, String name, Object args[]) {
                    try {
                        return super.invokeMethod(object, name, args);
                    } catch (MissingMethodException mme) {
                        return callGlobal(name, args, context);
                    }
                }

                public Object invokeStaticMethod(Object object, String name, Object args[]) {
                    try {
                        return super.invokeStaticMethod(object, name, args);
                    } catch (MissingMethodException mme) {
                        return callGlobal(name, args, context);
                    }
                }
            });
            return scriptObject.run();
        } catch (Exception e) {
            throw new ScriptException(e);
        }
    }

    Class getScriptClass(final String script) throws SyntaxException, CompilationFailedException, IOException {
        Class clazz = classMap.get(script);
        if (clazz != null) {
            return clazz;
        } else {
            java.io.InputStream stream = new ByteArrayInputStream(script.getBytes());
            clazz = loader.parseClass(stream, generateScriptName());
            classMap.put(script, clazz);
            return clazz;
        }
    }

    private Object invokeImpl(final Object thiz, final String name, final Object args[]) throws ScriptException, NoSuchMethodException {
        if (name == null) {
            throw new NullPointerException("Method name can not be null");
        }
        try {
            if (thiz != null) {
                return InvokerHelper.invokeMethod(thiz, name, args);
            }
        } catch (MissingMethodException mme) {
            throw new NoSuchMethodException(mme.getMessage());
        } catch (Exception e) {
            throw new ScriptException(e);
        }
        return callGlobal(name, args);
    }

    private Object callGlobal(final String name, final Object args[]) {
        return callGlobal(name, args, context);
    }

    private Object callGlobal(final String name, final Object args[], final ScriptContext ctx) {
        Closure closure = globalClosures.get(name);
        if (closure != null) {
            return closure.call(args);
        }
        Object value = ctx.getAttribute(name);
        if (value instanceof Closure) {
            return ((Closure) value).call(args);
        } else {
            throw new MissingMethodException(name, getClass(), args);
        }
    }

    private synchronized String generateScriptName() {
        return "Script" + ++counter + ".groovy";
    }

    private Object makeInterface(final Object obj, final Class clazz) {
        if (clazz == null || !clazz.isInterface()) {
            throw new IllegalArgumentException("interface Class expected");
        } else {
            return Proxy.newProxyInstance(clazz.getClassLoader(), new Class[]{clazz}, new InvocationHandler() {
                public Object invoke(Object proxy, Method m, Object args[]) throws Throwable {
                    return invokeImpl(obj, m.getName(), args);
                }
            });
        }
    }

    private ClassLoader getParentLoader() {
        final ClassLoader ctxtLoader = Thread.currentThread().getContextClassLoader();
        try {
            final Class c = ctxtLoader.loadClass("groovy.lang.Script");
            if (c == groovy.lang.Script.class) {
                return ctxtLoader;
            }
        } catch (ClassNotFoundException e) {
        }
        return groovy.lang.Script.class.getClassLoader();
    }

    private String readFully(final Reader reader) throws ScriptException {
        char arr[] = new char[8192];
        StringBuilder buf = new StringBuilder();
        int numChars;
        try {
            while ((numChars = reader.read(arr, 0, arr.length)) > 0) {
                buf.append(arr, 0, numChars);
            }
        } catch (IOException exp) {
            throw new ScriptException(exp);
        }
        return buf.toString();
    }
}
