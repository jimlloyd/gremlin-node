package com.entrendipity.gremlinnode.function;

import com.tinkerpop.gremlin.groovy.jsr223.GremlinGroovyScriptEngine;
import groovy.lang.Closure;
import javax.script.ScriptException;
import org.junit.Before;
import org.junit.Test;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

public class GroovyPredicateTest {

    GremlinGroovyScriptEngine engine;

    @Before
    public void initEngine() {
        engine = new GremlinGroovyScriptEngine();
    }

    @Test
    public void trivialClosureShouldReturnTrue() {
        try {
            final Closure closure = (Closure) engine.eval("{ x -> true }");
            final GroovyPredicate<Integer> predicate = new GroovyPredicate(closure);
            assertEquals(true, predicate.test(0));
        }
        catch (ScriptException se) {
            assertTrue(false);
        }
    }

    @Test
    public void trivialClosureShouldReturnFalse() {
        try {
            final Closure closure = (Closure) engine.eval("{ x -> false }");
            final GroovyPredicate<Integer> predicate = new GroovyPredicate(closure);
            assertEquals(false, predicate.test(0));
        }
        catch (ScriptException se) {
            assertTrue(false);
        }
    }

    @Test
    public void simpleClosureWorks() {
        try {
            final Closure closure = (Closure) engine.eval("{ x -> x < 100 }");
            final GroovyPredicate<Integer> predicate = new GroovyPredicate(closure);
            assertTrue(predicate.test(0));
            assertTrue(predicate.test(99));
            assertFalse(predicate.test(100));
            assertFalse(predicate.test(999));
        }
        catch (ScriptException se) {
            assertTrue(false);
        }
    }
}
