'use strict';

var _ = require('lodash');
var assert = require('assert');
var path = require('path');
var Q = require('q');
var Gremlin = require('../lib/gremlin');
var GraphWrapper = require('../lib/graph-wrapper');
var PipelineWrapper = require('../lib/pipeline-wrapper');
var VertexWrapper = require('../lib/vertex-wrapper');
var EdgeWrapper = require('../lib/edge-wrapper');

// For reference, see the java interface:
// https://github.com/tinkerpop/gremlin/blob/master/gremlin-java/src/main/java/com/tinkerpop/gremlin/java/GremlinFluentPipeline.java

function verticesMapToStrings(verts) {
  return _.map(verts, function (v) {
    assert.ok(v);
    assert.ok(v instanceof VertexWrapper);
    return v.toStringSync();
  });
}

function edgesMapToStrings(edges) {
  return _.map(edges, function (e) {
    assert.ok(e);
    assert.ok(e instanceof EdgeWrapper);
    return e.toStringSync();
  });
}

suite('pipeline-wrapper', function () {
  var gremlin;
  var java;
  var graph;
  var g;

  suiteSetup(function () {
    gremlin = new Gremlin();
    java = gremlin.java;
  });

  setup(function () {
    var TinkerGraphFactory = gremlin.java.import('com.tinkerpop.gremlin.tinkergraph.structure.TinkerFactory');
    graph = TinkerGraphFactory.createClassicSync();
    g = new GraphWrapper(gremlin, graph);
  });

  test('V().next()', function (done) {
    var traversal = g.V();
    assert.ok(traversal instanceof PipelineWrapper);

    traversal.next(function (err, v) {
      assert.ifError(err);
      assert.ok(v instanceof VertexWrapper);

      v.getProperty('name', function (err, name) {
        assert.ifError(err);
        assert.strictEqual(name, 'marko');
        done();
      });
    });
  });

  test('V().next() -> toJSON', function (done) {
    var traversal = g.V();
    assert.ok(traversal instanceof PipelineWrapper);

    traversal.next(function (err, v) {
      assert.ifError(err);
      assert.ok(v instanceof VertexWrapper);

      gremlin.toJsObj(v, function (err, jsonObj) {
        assert.ifError(err);
        // console.log(require('util').inspect(jsonObj, {depth: null}));
        var expected = {
          id: 1,
          label: 'vertex',
          type: 'vertex',
          properties:
          {
            name: [ { id: 0, label: 'name', value: 'marko' } ],
            age: [ { id: 1, label: 'age', value: 29 } ]
          }
        };

        assert.deepEqual(jsonObj, expected);
        done();
      });
    });
  });

  test('V().has(key, value)', function (done) {
    var traversal = g.V().has('name', 'josh');
    assert.ok(traversal instanceof PipelineWrapper);

    traversal.next(function (err, v) {
      assert.ifError(err);
      assert.ok(v instanceof VertexWrapper);

      v.getProperty('name', function (err, name) {
        assert.ifError(err);
        assert.strictEqual(name, 'josh');
        done();
      });
    });
  });

  test('V().toArray()', function (done) {
    var traversal = g.V();
    assert.ok(traversal instanceof PipelineWrapper);

    traversal.toArray(function (err, a) {
      assert.ifError(err);
      assert.ok(_.isArray(a));
      var vstrs = _.map(a, function (v) { return v.toStringSync(); });
      assert.deepEqual(vstrs, [ 'v[1]', 'v[2]', 'v[3]', 'v[4]', 'v[5]', 'v[6]' ]);
      done();
    });
  });

  test('V().has(key, value).toArray()', function (done) {
    var traversal = g.V().has('name', 'josh');
    assert.ok(traversal instanceof PipelineWrapper);

    traversal.toArray(function (err, a) {
      assert.ifError(err);
      assert.ok(_.isArray(a));
      var vstrs = _.map(a, function (v) { return v.toStringSync(); });
      assert.deepEqual(vstrs, [ 'v[4]' ]);
      done();
    });
  });

  test('g.V().toArray() -> toJsObj()', function (done) {
    var traversal = g.V();
    assert.ok(traversal instanceof PipelineWrapper);

    traversal.toArray(function (err, arr) {
      assert.ifError(err);

      gremlin.toJsObj(arr, function (err, verts) {
        // console.log(require('util').inspect(verts, {depth: null}));
        var expected = [
          { id: 1,
            label: 'vertex',
            type: 'vertex',
            properties:
             { name: [ { id: 0, label: 'name', value: 'marko' } ],
               age: [ { id: 1, label: 'age', value: 29 } ] } },
          { id: 2,
            label: 'vertex',
            type: 'vertex',
            properties:
             { name: [ { id: 2, label: 'name', value: 'vadas' } ],
               age: [ { id: 3, label: 'age', value: 27 } ] } },
          { id: 3,
            label: 'vertex',
            type: 'vertex',
            properties:
             { name: [ { id: 4, label: 'name', value: 'lop' } ],
               lang: [ { id: 5, label: 'lang', value: 'java' } ] } },
          { id: 4,
            label: 'vertex',
            type: 'vertex',
            properties:
             { name: [ { id: 6, label: 'name', value: 'josh' } ],
               age: [ { id: 7, label: 'age', value: 32 } ] } },
          { id: 5,
            label: 'vertex',
            type: 'vertex',
            properties:
             { name: [ { id: 8, label: 'name', value: 'ripple' } ],
               lang: [ { id: 9, label: 'lang', value: 'java' } ] } },
          { id: 6,
            label: 'vertex',
            type: 'vertex',
            properties:
             { name: [ { id: 10, label: 'name', value: 'peter' } ],
               age: [ { id: 11, label: 'age', value: 35 } ] } }
        ];
        assert.deepEqual(verts, expected);
        done();
      });
    });
  });

  test('g.E().next()', function (done) {
    var traversal = g.E();
    assert.ok(traversal instanceof PipelineWrapper);

    traversal.next(function (err, e) {
      assert.ifError(err);
      assert.ok(e instanceof EdgeWrapper);
      assert.strictEqual(e.toStringSync(), 'e[7][1-knows->2]');
      done();
    });
  });

  test('g.E().toArray() -> map(toStringSync)', function (done) {
    var traversal = g.E();
    assert.ok(traversal instanceof PipelineWrapper);

    traversal.toArray(function (err, edges) {
      assert.ifError(err);
      assert.ok(_.isArray(edges));
      var estrs = edgesMapToStrings(edges);
      assert.deepEqual(estrs, [
        'e[7][1-knows->2]',
        'e[8][1-knows->4]',
        'e[9][1-created->3]',
        'e[10][4-created->5]',
        'e[11][4-created->3]',
        'e[12][6-created->3]'
      ]);
      done();
    });
  });

  test('g.E().toArray() -> toJsObj', function (done) {
    var traversal = g.E();
    assert.ok(traversal instanceof PipelineWrapper);

    traversal.toArray()
      .then(gremlin.toJsObj.bind(gremlin), assert.ifError)
      .then(function (edges) {
        assert.ok(_.isArray(edges));
        // console.log(require('util').inspect(edges, {depth: null}));
        var expected = [
          { inV: 2,
            inVLabel: 'vertex',
            outVLabel: 'vertex',
            id: 7,
            label: 'knows',
            type: 'edge',
            outV: 1,
            properties: { weight: 0.5 } },
          { inV: 4,
            inVLabel: 'vertex',
            outVLabel: 'vertex',
            id: 8,
            label: 'knows',
            type: 'edge',
            outV: 1,
            properties: { weight: 1 } },
          { inV: 3,
            inVLabel: 'vertex',
            outVLabel: 'vertex',
            id: 9,
            label: 'created',
            type: 'edge',
            outV: 1,
            properties: { weight: 0.4 } },
          { inV: 5,
            inVLabel: 'vertex',
            outVLabel: 'vertex',
            id: 10,
            label: 'created',
            type: 'edge',
            outV: 4,
            properties: { weight: 1 } },
          { inV: 3,
            inVLabel: 'vertex',
            outVLabel: 'vertex',
            id: 11,
            label: 'created',
            type: 'edge',
            outV: 4,
            properties: { weight: 0.4 } },
          { inV: 3,
            inVLabel: 'vertex',
            outVLabel: 'vertex',
            id: 12,
            label: 'created',
            type: 'edge',
            outV: 6,
            properties: { weight: 0.2 } }
        ];
        assert.deepEqual(edges, expected);
      }, assert.ifError)
      .done(done);
  });

  test('g.E().has(key, val)', function (done) {
    g.E().has(gremlin.T.id, 7).toArray()
      .then(function (arr) { return gremlin.toJsObj(arr); }, assert.ifError)
      .then(function (edges) {
        assert.strictEqual(edges.length, 1);
        // console.log(require('util').inspect(edges, {depth: null}));
        var expected = [
          { inV: 2,
            inVLabel: 'vertex',
            outVLabel: 'vertex',
            id: 7,
            label: 'knows',
            type: 'edge',
            outV: 1,
            properties: { weight: 0.5 } }
        ];
        assert.deepEqual(edges, expected);
      }, assert.ifError)
      .done(done);
  });

  test('g.E().has(weight, gt, 0.5)', function (done) {
    g.E().has('weight', gremlin.Compare.gt, java.newFloat(0.5)).next(function (err, e) {
      assert.ifError(err);
      assert.ok(e instanceof EdgeWrapper);
      e.getProperty('weight', function (err, weight) {
        assert.ifError(err);
        assert(weight > 0.5);
        assert.strictEqual(e.toStringSync(), 'e[8][1-knows->4]');
        done();
      });
    });
  });

  test('has(string key, object value)', function (done) {
    g.V().has('name', 'marko').next(function (err, v) {
      assert.ifError(err);
      v.getProperty('name', function (err, name) {
        assert.ifError(err);
        assert.strictEqual(name, 'marko');
        done();
      });
    });
  });

  test('has(string key, object value).count()', function (done) {
    g.V().has('name', 'marko').count().next(function (err, count) {
      assert.ifError(err);
      assert.strictEqual(count, 1);
      done();
    });
  });

  test('has(string key, token, object value)', function (done) {
    g.V().has('name', gremlin.Compare.eq, 'marko').next(function (err, v) {
      assert.ifError(err);
      v.getProperty('name', function (err, name) {
        assert.ifError(err);
        assert.strictEqual(name, 'marko');
        done();
      });
    });
  });

  test('hasNot(string key, object value)', function (done) {
    g.V().hasNot('age').count().next(function (err, count) {
      assert.ifError(err);
      assert.strictEqual(count, 2);
      done();
    });
  });

  // hasNot(key, val) not in TK3. has(key, Compare.ne, val) doesn't work the same.
  // test.skip('hasNot(string key, object value)', function (done) {
  //   g.V().hasNot('age', 27).count().next(function (err, count) {
  //     assert.ifError(err);
  //     assert.strictEqual(count, 5);
  //     done();
  //   });
  // });

  test('interval(string key, object start, object end)', function (done) {
    var lower = 0.3;
    var upper = 0.9;

    var pipe = g.E().interval('weight', java.newFloat(lower), java.newFloat(upper));
    pipe.toArray()
      .then(function (a) {
        assert(_.isArray(a));
        assert.strictEqual(a.length, 3);
        var p = a.map(function (e) { return e.getProperty('weight'); });
        Q.all(p)
          .then(function (weights) {
            weights.map(function (w) {
              assert(w >= lower && w <= upper);
            });
          })
          .done(done);
      })
      .done();
  });

  test('bothE(string... labels)', function (done) {
    g.V().bothE('knows', 'created').toArray(function (err, edges) {
      assert.ifError(err);
      assert.strictEqual(edges.length, 12);
      var counts = _.countBy(edges, function (e) { return e.getLabel(); });
      var expected = { created: 8, knows: 4 };
      assert.deepEqual(counts, expected);
      done();
    });
  });

  test('bothE(int branchFactor, string... labels)', function (done) {
    g.V().bothE(1, 'knows', 'created').toArray(function (err, edges) {
      assert.ifError(err);
      assert.strictEqual(edges.length, 6);
      var counts = _.countBy(edges, function (e) { return e.getLabel(); });
      // var expected = { created: 3, knows: 3 };  in TK2 unit tests, was 3,3
      var expected = { created: 4, knows: 2 };    // but in TK3, is now 4,2. WTF?? TODO: is this right?
      assert.deepEqual(counts, expected);
      done();
    });
  });

  test('both(string... labels).toArray()', function (done) {
    g.V().both('knows').toArray(function (err, verts) {
      assert.strictEqual(verts.length, 4);
      var strs = verticesMapToStrings(verts);
      assert.deepEqual(strs, ['v[2]', 'v[4]', 'v[1]', 'v[1]']);
      done();
    });
  });

  test('both(string... labels).dedup().toArray()', function (done) {
    g.V().both('knows').dedup().toArray(function (err, verts) {
      assert.ifError(err);
      assert.strictEqual(verts.length, 3);
      var strs = verticesMapToStrings(verts);
      assert.deepEqual(strs, ['v[2]', 'v[4]', 'v[1]']);
      done();
    });
  });

  test('both(int branchFactor, string... labels)', function (done) {
    g.V().both(1, 'knows').dedup().toArray(function (err, verts) {
      assert.ifError(err);
      assert.strictEqual(verts.length, 2);
      var strs = verticesMapToStrings(verts);
      assert.deepEqual(strs, ['v[2]', 'v[1]']);
      done();
    });
  });

  test('bothV()', function (done) {
    g.E().has(gremlin.T.id, 7).bothV().toArray()
      .then(function (verts) {
        var strs = verticesMapToStrings(verts);
        assert.deepEqual(strs, ['v[1]', 'v[2]']);
      }, assert.ifError)
      .done(done);
  });

  test('inV()', function (done) {
    g.E().has(gremlin.T.id, 7).inV().toArray()
      .then(function (verts) {
        var strs = verticesMapToStrings(verts);
        assert.deepEqual(strs, ['v[2]']);
      }, assert.ifError)
      .done(done);
  });

  test('inE()', function (done) {
    g.V().has('name', 'lop').inE().toArray()
      .then(function (edges) {
        var strs = edgesMapToStrings(edges);
        assert.deepEqual(strs, ['e[9][1-created->3]', 'e[11][4-created->3]', 'e[12][6-created->3]']);
      }, assert.ifError)
      .done(done);
  });

  test('in()', function (done) {
    g.V().has('name', 'lop').in().toArray()
      .then(function (verts) {
        var strs = verticesMapToStrings(verts);
        assert.deepEqual(strs, ['v[1]', 'v[4]', 'v[6]']);
      }, assert.ifError)
      .done(done);
  });

  test('outV()', function (done) {
    g.E().has(gremlin.T.id, 7).outV().toArray()
      .then(function (verts) {
        var strs = verticesMapToStrings(verts);
        assert.deepEqual(strs, ['v[1]']);
      }, assert.ifError)
      .done(done);
  });

  test('outE()', function (done) {
    g.V().has('name', 'josh').outE().toArray(function (err, edges) {
      assert.ifError(err);
      assert.strictEqual(edges.length, 2);
      done();
    });
  });

  test('out()', function (done) {
    g.V().has('name', 'josh').out().toArray(function (err, edges) {
      assert.ifError(err);
      assert.strictEqual(edges.length, 2);
      done();
    });
  });

  test('id()', function (done) {
    g.V().id().toArray(function (err, ids) {
      assert.ifError(err);
      var expected = [ 1, 2, 3, 4, 5, 6 ];
      assert.deepEqual(ids, expected);
      g.E().id().toArray(function (err, ids) {
        assert.ifError(err);
        var expected = [ 7, 8, 9, 10, 11, 12 ];
        assert.deepEqual(ids, expected);
        done();
      });
    });
  });

  test('label()', function (done) {
    g.E().label().toArray(function (err, labels) {
      assert.ifError(err);
      var expected = [ 'knows', 'knows', 'created', 'created', 'created', 'created' ];
      assert.deepEqual(labels, expected);
      done();
    });
  });

  test('value()', function (done) {
    g.V().value('name').toArray(function (err, names) {
      assert.ifError(err);
      var expected = [ 'marko', 'vadas', 'lop', 'josh', 'ripple', 'peter' ];
      assert.deepEqual(names, expected);
      g.V().value('age').toArray(function (err, ages) {
        assert.ifError(err);
        var expected = [ 29, 27, 32, 35 ];
        assert.deepEqual(ages, expected);
        done();
      });
    });
  });

  // TODO
  // PipelineWrapper.prototype.idEdge = function () {
  // PipelineWrapper.prototype.id = function () {
  // PipelineWrapper.prototype.idVertex = function () {
  // PipelineWrapper.prototype.step = function () {

// No copySplit or fairMerge in TK3. What are their equivalents? jump() may provide copySplit() functionality.
//   test.skip('copySplit(), _(), and fairMerge()', function (done) {
//     g.V().both().toArray(function (err, bothed) {
//       g.V().copySplit(g._().in(), g._().out()).fairMerge().toArray(function (err, copied) {
//         assert.strictEqual(bothed.length, copied.length);
//         done();
//       });
//     });
//   });

  // PipelineWrapper.prototype.exhaustMerge = function () {
  // PipelineWrapper.prototype.fairMerge = function () {
  // PipelineWrapper.prototype.ifThenElse = function () {
  // PipelineWrapper.prototype.loop = function () {
  // PipelineWrapper.prototype.and = function (/*final Pipe<E, ?>... pipes*/) {
  test('as() and back()', function (done) {
    g.V().as('test').out('knows').back('test').toArray(function (err, recs) {
      assert.ifError(err);
      // assert(recs.length === 1);  // TODO: in TK2, there was 1 rec
      assert(recs.length === 2);    // TODO: but in TK3, there are 2 recs
      var vstrs = verticesMapToStrings(recs);
      assert.deepEqual(vstrs, ['v[1]', 'v[1]']);  // TODO: Did TK2 return just single v[1]?
      done();
    });
  });

  // PipelineWrapper.prototype.except = function () {

  test('sideEffect()', function (done) {
    this.timeout(5000); // A longer timeout is required on Travis
    var GPredicate = java.import('com.tinkerpop.gremlin.groovy.function.GPredicate');
    var closure = gremlin.getEngine().evalSync('{ it -> it.get().value("name") == "lop" }');
    var predicate = new GPredicate(closure);
    g.V().filter(predicate).toArray(function (err, recs) {
      assert.ifError(err);
      assert.strictEqual(recs.length, 1);
      var v = recs[0];
      assert.ok(v instanceof VertexWrapper);
      gremlin.toJsObj(v, function (err, jsonObj) {
        var expected = {
          id: 3,
          label: 'vertex',
          type: 'vertex',
          properties:
           { name: [ { id: 4, label: 'name', value: 'lop' } ],
             lang: [ { id: 5, label: 'lang', value: 'java' } ] }
        };
        assert.deepEqual(jsonObj, expected);
        done();
      });
    });
  });

  // PipelineWrapper.prototype.or = function (/*final Pipe<E, ?>... pipes*/) {
  // PipelineWrapper.prototype.random = function () {
  // PipelineWrapper.prototype.index = function (idx) {
  // PipelineWrapper.prototype.range = function () {
  // PipelineWrapper.prototype.retain = function (/*final Collection<E> collection*/) {
  // PipelineWrapper.prototype.simplePath = function () {

  test('aggregate()', function (done) {
    // In TK3, aggregate creates a BulkSet. In PipelineWrapper._jsify we currently transform
    // BulkSets into an array of object each containing a key and a count.
    // We may change this behavior in the future.
    g.V().aggregate().next(function (err, agg) {
      assert.ifError(err);
      assert.ok(_.isArray(agg));
      assert.strictEqual(agg.length, 6);
      _.forEach(agg, function (item) {
        assert.ok(item.key instanceof VertexWrapper);
        assert.strictEqual(item.count, 1);
      });
      done();
    });
  });

  // PipelineWrapper.prototype.optional = function () {
  // PipelineWrapper.prototype.groupBy = function (map, closure) {

  test('groupCount()', function (done) {
    g.E().label().groupCount().next(function (err, agg) {
      assert.ifError(err);
      var expected = { 'created': 4, 'knows': 2 };
      assert.deepEqual(agg, expected);
      done();
    });
  });

  // PipelineWrapper.prototype.linkOut = function () {
  // PipelineWrapper.prototype.linkIn = function () {
  // PipelineWrapper.prototype.linkBoth = function () {
  // PipelineWrapper.prototype.sideEffect = function () {
  test('store()', function (done) {
    g.V().has('lang', 'java').store().next(function (err, agg) {
      // See comment in test for aggregate() above.
      assert.ifError(err);
      assert.ok(agg);
      assert.ok(_.isArray(agg));
      assert.strictEqual(agg.length, 2);
      _.forEach(agg, function (item) {
        assert.ok(item.key instanceof VertexWrapper);
        assert.strictEqual(item.count, 1);
      });
      done();
    });
  });

  // PipelineWrapper.prototype.table = function () {
  // PipelineWrapper.prototype.tree = function () {
  // PipelineWrapper.prototype.gather = function () {
  // PipelineWrapper.prototype._ = function () {
  // PipelineWrapper.prototype.memoize = function () {
  // PipelineWrapper.prototype.order = function () {
  // PipelineWrapper.prototype.path = function () {
  // PipelineWrapper.prototype.scatter = function () {
  // PipelineWrapper.prototype.select = function () {
  // PipelineWrapper.prototype.shuffle = function () {

  test('groupCount() and cap()', function (done) {
    g.V().in().id().groupCount().cap().next(function (err, map) {
      assert.ifError(err);
      assert(map['1'] === 3);
      assert(map['4'] === 2);
      assert(map['6'] === 1);
      done();
    });
  });

  // PipelineWrapper.prototype.orderMap = function () {
  // PipelineWrapper.prototype.transform = function () {

});
