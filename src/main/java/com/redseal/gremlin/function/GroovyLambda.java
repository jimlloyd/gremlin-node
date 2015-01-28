package com.entrendipity.gremlinnode.function;

import com.tinkerpop.gremlin.process.computer.util.ScriptEngineCache;
import com.tinkerpop.gremlin.util.function.TriConsumer;
import groovy.lang.Closure;
import java.util.function.BiConsumer;
import java.util.function.Consumer;
import java.util.function.Function;
import java.util.function.Predicate;
import java.util.function.Supplier;
import javax.script.ScriptEngine;
import javax.script.ScriptException;

/**
 * Create a versatile lambda from a Groovy closure.
 *
 * Based on code written by Marko A. Rodriguez (http://markorodriguez.com)
 */
public class GroovyLambda implements Function, Supplier, Consumer, Predicate, BiConsumer, TriConsumer {

    private final String groovy;
    private final ScriptEngine engine;
    private final Closure closure;

    private final static String GROOVY_SCRIPT_ENGINE_NAME = "Groovy";

    public static ScriptEngine getDefaultEngine() {
        return ScriptEngineCache.get(GROOVY_SCRIPT_ENGINE_NAME);
    }

    public GroovyLambda(final String groovy) throws ScriptException {
        this(groovy, getDefaultEngine());
    }

    public GroovyLambda(final String groovy, final ScriptEngine engine) throws ScriptException {
        this.groovy = groovy;
        this.engine = engine;
        this.closure = (Closure) this.engine.eval(groovy);
    }

    // Function.apply
    @Override
    public Object apply(final Object a) {
        return this.closure.call(a);
    }

    // Supplier.get
    @Override
    public Object get() {
        return this.closure.call();
    }

    // Consumer.accept
    @Override
    public void accept(final Object a) {
        this.closure.call(a);
    }

    // BiConsumer.accept
    @Override
    public void accept(final Object a, final Object b) {
        this.closure.call(a, b);
    }

    // TriConsumer.accept
    @Override
    public void accept(final Object a, final Object b, final Object c) {
        this.closure.call(a, b, c);
    }

    // Predicate.test
    @Override
    public boolean test(final Object a) {
        return (boolean) this.closure.call(a);
    }
}
