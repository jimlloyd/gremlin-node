package com.entrendipity.gremlinnode.function;

import com.tinkerpop.gremlin.groovy.jsr223.GremlinGroovyScriptEngine;
import groovy.lang.Closure;
import java.util.HashSet;
import java.util.Set;
import javax.script.Bindings;
import javax.script.ScriptContext;
import javax.script.ScriptEngine;
import javax.script.ScriptException;
import org.junit.Test;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotEquals;
import static org.junit.Assert.assertTrue;

public class GroovyLambdaTest {

    // Function.apply

    @Test
    public void simpleFunctionWorks() {
        try {
            final GroovyLambda lambda = new GroovyLambda("{ x -> x + 2 }");
            assertEquals(lambda.apply(5), 7);
            assertEquals(lambda.apply("foo"), "foo2");
        }
        catch (ScriptException se) {
            assertTrue(se.toString(), false);
        }
    }

    // Supplier.get

    @Test
    public void simpleSupplierWorks() {
        try {
            final GroovyLambda lambda = new GroovyLambda("{ -> System.currentTimeMillis() }");
            assertNotEquals(lambda.get(), 0);
        }
        catch (ScriptException se) {
            assertTrue(se.toString(), false);
        }
    }

    // Consumer.accept

    @Test
    public void simpleConsumerWorks() {
        try {
            // Use our own engine so we can consume into a container.
            final ScriptEngine engine = newEngine();
            final Set<Long> set = new HashSet<Long>();
            engine.put("set", set);

            final GroovyLambda lambda = new GroovyLambda("{ it -> set.add(it) }", engine);

            lambda.accept(1);
            lambda.accept(2);
            lambda.accept(3);

            assertEquals(set.size(), 3);
        }
        catch (ScriptException se) {
            assertTrue(se.toString(), false);
        }
    }

    // BiConsumer.accept

    @Test
    public void simpleBiConsumerWorks() {
        try {
            // Use our own engine so we can consume into a container.
            final ScriptEngine engine = newEngine();
            final Set<Long[]> set = new HashSet<Long[]>();
            engine.put("set", set);

            final GroovyLambda lambda = new GroovyLambda("{ a, b -> set.add([a, b]) }", engine);

            lambda.accept(1, 2);
            lambda.accept(2, 3);
            lambda.accept(3, 4);

            assertEquals(set.size(), 3);
        }
        catch (ScriptException se) {
            assertTrue(se.toString(), false);
        }
    }

    // TriConsumer.accept

    @Test
    public void simpleTriConsumerWorks() {
        try {
            // Use our own engine so we can consume into a container.
            final ScriptEngine engine = newEngine();
            final Set<Long[]> set = new HashSet<Long[]>();
            engine.put("set", set);

            final GroovyLambda lambda = new GroovyLambda("{ a, b, c -> set.add([a, b, c]) }", engine);

            lambda.accept(1, 2, 3);
            lambda.accept(2, 3, 4);
            lambda.accept(3, 4, 5);

            assertEquals(set.size(), 3);
        }
        catch (ScriptException se) {
            assertTrue(se.toString(), false);
        }
    }

    // Predicate.test

    @Test
    public void trivialPredicateShouldReturnTrue() {
        try {
            final GroovyLambda lambda = new GroovyLambda("{ x -> true }");
            assertEquals(lambda.test(0), true);
        }
        catch (ScriptException se) {
            assertTrue(se.toString(), false);
        }
    }

    @Test
    public void trivialPredicateShouldReturnFalse() {
        try {
            final GroovyLambda lambda = new GroovyLambda("{ x -> false }");
            assertEquals(lambda.test(0), false);
        }
        catch (ScriptException se) {
            assertTrue(se.toString(), false);
        }
    }

    @Test
    public void simplePredicateWorks() {
        try {
            final GroovyLambda lambda = new GroovyLambda("{ x -> x < 100 }");
            assertTrue(lambda.test(0));
            assertTrue(lambda.test(99));
            assertFalse(lambda.test(100));
            assertFalse(lambda.test(999));
        }
        catch (ScriptException se) {
            assertTrue(se.toString(), false);
        }
    }

    // Utilities

    private ScriptEngine newEngine() {
        return new GremlinGroovyScriptEngine();
    }
}
