package com.entrendipity.gremlinnode.function;

import com.tinkerpop.gremlin.groovy.jsr223.GremlinGroovyScriptEngine;
import com.tinkerpop.gremlin.util.function.TriConsumer;
import groovy.lang.Closure;
import java.util.HashSet;
import java.util.Set;
import java.util.function.BiConsumer;
import java.util.function.Consumer;
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

    // Function.apply, UnaryOperator.apply

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

    // Function.andThen

    @Test
    public void simpleFunctionAndThenWorks() {
        try {
            final GroovyLambda original = new GroovyLambda("{ x -> x + 2 }");
            final GroovyLambda then = new GroovyLambda("{ x -> x * 10 }");
            final GlobFunction combined = original.andThen(then);
            assertEquals(combined.apply(5), 10 * (5 + 2));
        }
        catch (ScriptException se) {
            assertTrue(se.toString(), false);
        }
    }

    // BiFunction.apply, BinaryOperator.apply

    @Test
    public void simpleBiFunctionWorks() {
        try {
            final GroovyLambda lambda = new GroovyLambda("{ x, y -> x + y }");
            assertEquals(lambda.apply(5, 6), 11);
            assertEquals(lambda.apply("foo", "bar"), "foobar");
        }
        catch (ScriptException se) {
            assertTrue(se.toString(), false);
        }
    }

    // BiFunction.andThen

    @Test
    public void simpleBiFunctionAndThenWorks() {
        try {
            final GroovyLambda original = new GroovyLambda("{ x, y -> x + y }");
            final GroovyLambda then = new GroovyLambda("{ x -> x * 10 }");
            final GlobFunction combined = original.andThen(then);
            assertEquals(combined.apply(5, 2), 10 * (5 + 2));
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

    // Consumer.andThen

    @Test
    public void simpleConsumerAndThenWorks() {
        try {
            // Use our own engine so we can consume into a container.
            final ScriptEngine engine = newEngine();
            final Set<Long> set1 = new HashSet<Long>();
            final Set<Long> set2 = new HashSet<Long>();
            engine.put("set1", set1);
            engine.put("set2", set2);

            final Consumer first = new GroovyLambda("{ it -> set1.add(it) }", engine);
            final Consumer then = new GroovyLambda("{ it -> set2.add(it) }", engine);
            final Consumer combined = first.andThen(then);

            combined.accept(1);
            combined.accept(2);
            combined.accept(3);

            assertEquals(set1.size(), 3);
            assertEquals(set2.size(), 3);
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

    // BiConsumer.andThen

    @Test
    public void simpleBiConsumerAndThenWorks() {
        try {
            // Use our own engine so we can consume into a container.
            final ScriptEngine engine = newEngine();
            final Set<Long[]> set1 = new HashSet<>();
            final Set<Long[]> set2 = new HashSet<>();
            engine.put("set1", set1);
            engine.put("set2", set2);

            final BiConsumer first = new GroovyLambda("{ a, b -> set1.add([a, b]) }", engine);
            final BiConsumer then = new GroovyLambda("{ a, b -> set2.add([a, b]) }", engine);
            final BiConsumer combined = first.andThen(then);

            combined.accept(1, 2);
            combined.accept(2, 3);
            combined.accept(3, 4);

            assertEquals(set1.size(), 3);
            assertEquals(set2.size(), 3);
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

    // TriConsumer.andThen

    @Test
    public void simpleTriConsumerAndThenWorks() {
        try {
            // Use our own engine so we can consume into a container.
            final ScriptEngine engine = newEngine();
            final Set<Long[]> set1 = new HashSet<>();
            final Set<Long[]> set2 = new HashSet<>();
            engine.put("set1", set1);
            engine.put("set2", set2);

            final TriConsumer first = new GroovyLambda("{ a, b, c -> set1.add([a, b, c]) }", engine);
            final TriConsumer then = new GroovyLambda("{ a, b, c -> set2.add([a, b, c]) }", engine);
            final TriConsumer combined = first.andThen(then);

            combined.accept(1, 2, 3);
            combined.accept(2, 3, 4);
            combined.accept(3, 4, 5);

            assertEquals(set1.size(), 3);
            assertEquals(set2.size(), 3);
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

    // BiPredicate.test

    @Test
    public void simpleBiPredicateWorks() {
        try {
            final GroovyLambda lambda = new GroovyLambda("{ x, y -> x < y }");
            assertTrue(lambda.test(0, 5));
            assertTrue(lambda.test(99, 100));
            assertFalse(lambda.test(100, 100));
            assertFalse(lambda.test(999, 72));
        }
        catch (ScriptException se) {
            assertTrue(se.toString(), false);
        }
    }

    // Predicate.negate

    @Test
    public void simplePredicateNegateWorks() {
        try {
            final GroovyLambda original = new GroovyLambda("{ x -> x < 100 }");
            final GlobFunction negate = original.negate();
            assertFalse(negate.test(0));
            assertFalse(negate.test(99));
            assertTrue(negate.test(100));
            assertTrue(negate.test(999));
        }
        catch (ScriptException se) {
            assertTrue(se.toString(), false);
        }
    }

    // BiPredicate.negate

    @Test
    public void simpleBiPredicateNegateWorks() {
        try {
            final GroovyLambda original = new GroovyLambda("{ x, y -> x < y }");
            final GlobFunction negate = original.negate();
            assertFalse(negate.test(0, 5));
            assertFalse(negate.test(99, 100));
            assertTrue(negate.test(100, 100));
            assertTrue(negate.test(999, 72));
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
