package com.entrendipity.gremlinnode.function;

import groovy.lang.Closure;

import java.util.function.Predicate;

/**
 * Create a Predicate from a Groovy closure.
 *
 * Based on code written by Marko A. Rodriguez (http://markorodriguez.com)
 */
public class GroovyPredicate<T> implements Predicate<T> {

    private final Closure closure;

    public GroovyPredicate(final Closure closure) {
        this.closure = closure;
    }

    @Override
    public boolean test(T t) {
        return (boolean) this.closure.call(t);
    }
}
