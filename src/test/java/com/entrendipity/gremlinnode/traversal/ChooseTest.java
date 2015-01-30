package com.entrendipity.gremlinnode.traversal;

import com.entrendipity.gremlinnode.function.GroovyLambda;
import com.tinkerpop.gremlin.process.T;
import com.tinkerpop.gremlin.process.Traversal;
import com.tinkerpop.gremlin.process.util.MapHelper;
import com.tinkerpop.gremlin.structure.Graph;
import com.tinkerpop.gremlin.structure.Vertex;
import com.tinkerpop.gremlin.tinkergraph.structure.TinkerFactory;
import org.junit.Before;
import org.junit.Test;

import java.util.HashMap;
import java.util.Map;
import javax.script.ScriptException;

import static com.tinkerpop.gremlin.process.graph.AnonymousGraphTraversal.Tokens.__;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

public class ChooseTest {

    private Graph graph;

    @Before
    public void initGraph() {
        graph = TinkerFactory.createClassic();
    }

    @Test
    public void trivialChoosePredicateWorks() {
        final Traversal<Vertex, String> traversal =
            graph.V().choose(v -> false,
                             __.inject("foo"),
                             __.inject("bar"));

        Map<String, Long> counts = new HashMap<>();
        int counter = 0;
        while (traversal.hasNext()) {
            Object o = traversal.next();
            if (o instanceof Vertex) {
                Vertex v = (Vertex) o;
                o = v.property("name").value();
            }
            MapHelper.incr(counts, (String) o, 1l);
            counter++;
        }
        assertFalse(traversal.hasNext());
        assertEquals(7, counter);
        assertEquals(7, counts.size());
        assertEquals(Long.valueOf(1), counts.get("bar"));
        assertEquals(Long.valueOf(1), counts.get("ripple"));
        assertEquals(Long.valueOf(1), counts.get("vadas"));
        assertEquals(Long.valueOf(1), counts.get("josh"));
        assertEquals(Long.valueOf(1), counts.get("lop"));
        assertEquals(Long.valueOf(1), counts.get("marko"));
        assertEquals(Long.valueOf(1), counts.get("peter"));
    }

    @Test
    public void simpleChoosePredicateWorks() {
        final Traversal<Vertex, String> traversal =
            graph.V()
            .choose(v -> v.<String>value("name").length() == 5,
                   __.out(),
                   __.in())
            .values("name");

        Map<String, Long> counts = new HashMap<>();
        int counter = 0;
        while (traversal.hasNext()) {
            MapHelper.incr(counts, traversal.next(), 1l);
            counter++;
        }
        assertFalse(traversal.hasNext());
        assertEquals(9, counter);
        assertEquals(5, counts.size());
        assertEquals(Long.valueOf(1), counts.get("vadas"));
        assertEquals(Long.valueOf(3), counts.get("josh"));
        assertEquals(Long.valueOf(2), counts.get("lop"));
        assertEquals(Long.valueOf(2), counts.get("marko"));
        assertEquals(Long.valueOf(1), counts.get("peter"));
    }

    @Test
    public void simpleChooseFunctionWorks() {
        HashMap choices = new HashMap() {{
            put(5, __.in());
            put(4, __.out());
            put(3, __.both());
        }};

        final Traversal<Vertex, String> traversal =
            graph.V()
            .has("age")
            .choose(v -> v.<String>value("name").length(), choices)
            .values("name");

        Map<String, Long> counts = new HashMap<>();
        int counter = 0;
        while (traversal.hasNext()) {
            MapHelper.incr(counts, traversal.next(), 1l);
            counter++;
        }
        assertFalse(traversal.hasNext());
        assertEquals(3, counter);
        assertEquals(3, counts.size());
        assertEquals(Long.valueOf(1), counts.get("marko"));
        assertEquals(Long.valueOf(1), counts.get("lop"));
        assertEquals(Long.valueOf(1), counts.get("ripple"));
    }

    @Test
    public void groovyChooseFunctionWorks() {
        HashMap choices = new HashMap() {{
            put(5, __.in());
            put(4, __.out());
            put(3, __.both());
        }};

        GroovyLambda lambda;
        try {
            lambda = new GroovyLambda("{ vertex -> vertex.value('name').length() }");
        }
        catch (ScriptException exc) {
            assertTrue(exc.toString(), false);
            return;
        }

        final Traversal<Vertex, String> traversal =
            graph.V()
            .has("age")
            .choose(lambda, choices)
            .values("name");

        Map<String, Long> counts = new HashMap<>();
        int counter = 0;
        while (traversal.hasNext()) {
            MapHelper.incr(counts, traversal.next(), 1l);
            counter++;
        }
        assertFalse(traversal.hasNext());
        assertEquals(3, counter);
        assertEquals(3, counts.size());
        assertEquals(Long.valueOf(1), counts.get("marko"));
        assertEquals(Long.valueOf(1), counts.get("lop"));
        assertEquals(Long.valueOf(1), counts.get("ripple"));
    }
}
