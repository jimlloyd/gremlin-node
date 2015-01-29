'use strict';

var _ = require('lodash');
var assert = require('assert');
var fs = require('fs');
var sinon = require('sinon');
var tmp = require('tmp');
var Q = require('q');
var Gremlin = require('../lib/gremlin');
var GraphWrapper = require('../lib/graph-wrapper');
var VertexWrapper = require('../lib/vertex-wrapper');
var EdgeWrapper = require('../lib/edge-wrapper');
var TraversalWrapper = require('../lib/traversal-wrapper');

Q.longStackSupport = true;

suite('graph-wrapper', function () {
  var gremlin;
  var graph;
  var g;
  var sandbox;

  suiteSetup(function () {
    gremlin = new Gremlin();
  });

  setup(function () {
    var TinkerGraphFactory = gremlin.java.import('com.tinkerpop.gremlin.tinkergraph.structure.TinkerFactory');
    graph = TinkerGraphFactory.createClassicSync();
    g = new GraphWrapper(gremlin, graph);
    sandbox = sinon.sandbox.create();
  });

  teardown(function () {
    sandbox.restore();
  });

  test('Non ThreadedTransactionalGraph instances do not start unique transactions', function () {
    graph.newTransactionSync = sandbox.spy();
    g.addVertex({}, function () {});
    assert(!graph.newTransactionSync.called);
  });

  test('ThreadedTransactionalGraph starts unique transactions', function () {
    var fakeTxn = {
      addVertex: sandbox.stub()
    };
    var fakeTx = {
      createSync: sandbox.stub().returns(fakeTxn)
    };
    var fakeGraph = {
      txSync: sandbox.stub().returns(fakeTx)
    };
    var fakeArray = {
    };
    var fakeJava = {
      newArray: sandbox.stub().returns(fakeArray)
    };
    var fakeGremlin = {
      isType: function () {},
      toList: function () {},
      toListSync: function () {},
      toJSON: function () {},
      toJSONSync: function () {},
      propertiesToVarArgs: function () {},
      java: fakeJava
    };
    var fakeArrayList = {
    };
    var g2 = new GraphWrapper(fakeGremlin, fakeGraph);

    var _supportsTransactionsStub = sinon.stub(g2, '_supportsTransactions').returns(true);

    // should start a new transaction
    g2.addVertex({}, function () {});

    // should re-use the existing transaction
    g2.addVertex({}, function () {});

    assert(_supportsTransactionsStub.calledOnce);
    assert(fakeGraph.txSync.calledOnce);
    assert(fakeTxn.addVertex.calledTwice);
  });

  test('addVertex({}) using callback API', function (done) {
    g.addVertex({}, function (err, v) {
      assert.ifError(err);
      assert(v instanceof VertexWrapper);
      done();
    });
  });

  test('addVertex({}) using promise API', function (done) {
    g.addVertex({})
      .then(function (v) { assert(v instanceof VertexWrapper); }, assert.ifError)
      .done(done);
  });

  test('getVertex(id) using callback API', function (done) {
    g.addVertex({name: 'jim'})
      .then(function (v) { assert(v instanceof VertexWrapper); return v; }, assert.ifError)
      .then(function (v) {
        var id = v.getId();
        g.getVertex(id, function (err, v) {
          assert.ifError(err);
          assert(v instanceof VertexWrapper);
          assert.strictEqual(v.toStringSync(), 'v[12]');
          v.value('name', function (err, name) {
            assert.ifError(err);
            assert.strictEqual(name, 'jim');
          });
        });
      })
      .done(done);
  });

  test('getVertex(id) using promise API', function (done) {
    g.addVertex({name: 'jim'})
      .then(function (v) { return v.getId(); }, assert.ifError)
      .then(function (id) { return g.getVertex(id); }, assert.ifError)
      .then(function (v) { return v.value('name'); }, assert.ifError)
      .then(function (name) {
        assert.strictEqual(name, 'jim');
      }, assert.ifError)
      .done(done);
  });

  test('v.remove() using callback API', function (done) {
    var saveId;
    g.addVertex({name: 'jim'})
      .then(function (v) { return v.getId(); }, assert.ifError)
      .then(function (id) { saveId = id; return null; }, assert.ifError)
      .then(function () { return g.getVertex(saveId); }, assert.ifError)
      .then(function (v) {
        v.remove(function (err) {
          done(err);
        });
      });
  });

  test('v.remove() using promise API', function (done) {
    var saveId;
    g.addVertex({name: 'jim'})
      .then(function (v) { return v.getId(); }, assert.ifError)
      .then(function (id) { saveId = id; return null; }, assert.ifError)
      .then(function () { return g.getVertex(saveId); }, assert.ifError)
      .then(function (v) { return v.remove(); }, assert.ifError)
      .done(done);
  });

  test('v.addEdge(label, v2) using callback API', function (done) {
    var v1, v2;
    g.getVertex(1)
      .then(function (v) {
        v1 = v;
        assert(v1 instanceof VertexWrapper);
        return g.getVertex(2);
      })
      .then(function (v) {
        v2 = v;
        assert(v2 instanceof VertexWrapper);
        return v1.addEdge('buddy', v2, {}, function (err, e) {
          assert.ifError(err);
          assert(e instanceof EdgeWrapper);
        });
      })
      .done(done);
  });

  test('v.addEdge(label, v2) using promise API', function (done) {
    var v1, v2;
    g.getVertex(1)
      .then(function (v) {
        v1 = v;
        assert(v1 instanceof VertexWrapper);
        return g.getVertex(2);
      })
      .then(function (v) {
        v2 = v;
        assert(v2 instanceof VertexWrapper);
        return v1.addEdge('buddy', v2, {});
      })
      .then(function (e) {
        assert(e instanceof EdgeWrapper);
      })
      .done(done);
  });

  test('getEdge(id) using callback API', function (done) {
    g.getEdge(7, function (err, e) {
      assert.ifError(err);
      assert(e instanceof EdgeWrapper);
      assert.strictEqual(e.getId(), 7);
      assert.strictEqual(e.toStringSync(), 'e[7][1-knows->2]');
      assert.strictEqual(e.getLabel(), 'knows');
      e.value('weight')
        .then(function (weight) {
          assert(weight > 0.0 && weight < 1.0);
        }, assert.ifError)
        .done(done);
    });
  });

  test('getEdge(id) using promise API', function (done) {
    g.getEdge(7)
      .then(function (e) {
        assert(e instanceof EdgeWrapper);
        assert.strictEqual(e.getId(), 7);
        assert.strictEqual(e.toStringSync(), 'e[7][1-knows->2]');
        assert.strictEqual(e.getLabel(), 'knows');
      }, assert.ifError)
      .done(done);
  });

  test('removeEdge(e) using callback API', function (done) {
    g.getEdge(7, function (err, e) {
      assert.ifError(err);
      assert(e instanceof EdgeWrapper);

      e.remove(function (err) {
        assert.ifError(err);

        g.getEdge(7)
          .then(function (edge) {
            done(new Error('should have thrown'));
          })
          .catch(function (err) {
            assert.ok(err.toString().match(/The edge with id 7 of type Integer does not exist in the graph/));
            done();
          });
      });
    });
  });

  test('removeEdge(e) using promise API', function (done) {
    g.getEdge(7)
      .then(function (e) {assert(e instanceof EdgeWrapper); return e.remove(); })
      .then(function () { return g.getEdge(7); })
      .then(function (e) { done(new Error('should have thrown')); })
      .catch(function (err) {
        assert.ok(err.toString().match(/The edge with id 7 of type Integer does not exist/));
        done();
      });
  });

  test('setProperty(key, value) / value(key) using callback API', function (done) {
    g.getVertex(1, function (err, v) {
      assert.ifError(err);
      assert(v instanceof VertexWrapper);
      v.setProperty('fruit', 'lemon', function (err) {
        assert.ifError(err);
        v.value('fruit', function (err, name) {
          assert.ifError(err);
          assert.strictEqual(name, 'lemon');
          done();
        });
      });
    });
  });

  test('setProperty(key, value) / value(key) using promise API', function (done) {
    var v;
    g.getVertex(1)
      .then(function (_v) { v = _v; assert(v instanceof VertexWrapper); return v; }, assert.ifError)
      .then(function () { return v.value('name'); }, assert.ifError)
      .then(function (name) { assert.strictEqual(name, 'marko'); return v; }, assert.ifError)
      .then(function () { return v.setProperty('name', 'john'); }, assert.ifError)
      .then(function () { return v.value('name'); }, assert.ifError)
      .then(function (name) { assert.strictEqual(name, 'john'); }, assert.ifError)
      .done(done);
  });

  test('setProperty(key, value) / valueSync(key) using promise API', function (done) {
    var v;
    g.getVertex(1)
      .then(function (_v) { v = _v; assert(v instanceof VertexWrapper); return v; }, assert.ifError)
      .then(function () { return v.value('name'); }, assert.ifError)
      .then(function (name) { assert.strictEqual(name, 'marko'); return v; }, assert.ifError)
      .then(function () { return v.setProperty('name', 'john'); }, assert.ifError)
      .then(function () { assert.strictEqual(v.valueSync('name'), 'john'); }, assert.ifError)
      .done(done);
  });

  test('setProperties(props) / values(props) using callback API', function (done) {
    g.getVertex(1, function (err, v) {
      assert.ifError(err);
      assert(v instanceof VertexWrapper);
      var expectedProps = { 'name': 'josh', 'age': 45, 'foo': 23, 'bar': 42, 'xxx': 'yyy' };
      v.setProperties(expectedProps, function (err) {
        assert.ifError(err);
        v.values(Object.keys(expectedProps), function (err, props) {
          assert.ifError(err);
          assert.deepEqual(props, expectedProps);
          done();
        });
      });
    });
  });

  test('setProperties(props) / values(props) using promise API', function (done) {
    g.getVertex(1, function (err, v) {
      assert.ifError(err);
      assert(v instanceof VertexWrapper);
      var expectedProps = { 'name': 'josh', 'age': 45, 'foo': 23, 'bar': 42, 'xxx': 'yyy' };
      v.setProperties(expectedProps)
        .then(function () { return v.values(Object.keys(expectedProps)); }, assert.ifError)
        .then(function (props) { assert.deepEqual(props, expectedProps); }, assert.ifError)
        .done(done);
    });
  });

  test('removeProperty(key) using callback API', function (done) {
    g.getVertex(1, function (err, v) {
      assert.ifError(err);
      assert(v instanceof VertexWrapper);
      v.removeProperty('name', function (err, res) {
        assert.ifError(err);
        v.value('name', function (err, name) {
          assert.ifError(err);
          assert.strictEqual(name, undefined);
          done();
        });
      });
    });
  });

  test('removeProperty(key) using promises API', function (done) {
    var v;
    g.getVertex(1)
      .then(function (_v) { v = _v; assert(v instanceof VertexWrapper); return v; }, assert.ifError)
      .then(function () { return v.removeProperty('name'); }, assert.ifError)
      .then(function () { return v.value('name'); }, assert.ifError)
      .then(function (name) { assert.strictEqual(name, undefined); }, assert.ifError)
      .done(done);
  });

  test('removeProperties(props) using callback API', function (done) {
    g.getVertex(1, function (err, v) {
      assert.ifError(err);
      assert(v instanceof VertexWrapper);
      v.removeProperties(['name', 'age'], function (err) {
        assert.ifError(err);
        v.values(['name', 'age'], function (err, props) {
          assert.ifError(err);
          assert.deepEqual(props, {name: undefined, age: undefined});
          done();
        });
      });
    });
  });

  test('removeProperties(props) using promises API', function (done) {
    var v;
    g.getVertex(1)
      .then(function (_v) { v = _v; assert(v instanceof VertexWrapper); return v; }, assert.ifError)
      .then(function () { return v.values(['name', 'age']); }, assert.ifError)
      .then(function (props) { assert.deepEqual(props, {name: 'marko', age: 29}); }, assert.ifError)
      .then(function () { return v.removeProperties(['name', 'age']); }, assert.ifError)
      .then(function () { return v.values(['name', 'age']); }, assert.ifError)
      .then(function (props) { assert.deepEqual(props, {name: undefined, age: undefined}); }, assert.ifError)
      .done(done);
  });

  test('V(2)', function (done) {
    var traversal = g.V(2);
    assert(traversal instanceof TraversalWrapper);
    traversal.toArray()
      .then(function (arr) {
        assert(_.isArray(arr));
        assert.strictEqual(arr.length, 1);
        var v = arr[0];
        assert(v instanceof VertexWrapper);
        assert.strictEqual(v.toStringSync(), 'v[2]');
      })
      .done(done);
  });

  test('V(2, 3)', function (done) {
    var traversal = g.V(2, 3);
    assert(traversal instanceof TraversalWrapper);
    traversal.toArray()
      .then(function (arr) {
        assert(_.isArray(arr));
        assert.strictEqual(arr.length, 2);
        var expected = ['v[2]', 'v[3]'];
        var actual = arr.map(function (v) {
          assert(v instanceof VertexWrapper);
          return v.toStringSync();
        });
        assert.deepEqual(expected.sort(), actual.sort());
      })
      .done(done);
  });

  test('V(id) with invalid ID', function (done) {
    var traversal = g.V(99);
    assert(traversal instanceof TraversalWrapper);
    traversal.toArray()
      .then(function (arr) {
        assert.deepEqual([], arr);
      })
      .done(done);
  });

  test('E(7)', function (done) {
    var traversal = g.E(7);
    assert(traversal instanceof TraversalWrapper);
    traversal.toArray()
      .then(function (arr) {
        assert(_.isArray(arr));
        assert.strictEqual(arr.length, 1);
        var e = arr[0];
        assert(e instanceof EdgeWrapper);
        assert.strictEqual(e.toStringSync(), 'e[7][1-knows->2]');
      })
      .done(done);
  });

  test('E(7, 8)', function (done) {
    var traversal = g.E(7, 8);
    assert(traversal instanceof TraversalWrapper);
    traversal.toArray()
      .then(function (arr) {
        assert(_.isArray(arr));
        assert.strictEqual(arr.length, 2);
        var expected = ['e[7][1-knows->2]', 'e[8][1-knows->4]'];
        var actual = arr.map(function (e) {
          assert(e instanceof EdgeWrapper);
          return e.toStringSync();
        });
        assert.deepEqual(expected.sort(), actual.sort());
      })
      .done(done);
  });

  test('E(id) with invalid ID', function (done) {
    var traversal = g.E(99);
    assert(traversal instanceof TraversalWrapper);
    traversal.toArray()
      .then(function (arr) {
        assert.deepEqual([], arr);
      })
      .done(done);
  });

  test('g.toString() using callback API', function (done) {
    var expected = 'tinkergraph[vertices:6 edges:6]';
    g.toString(function (err, str) {
      assert.ifError(err);
      assert.strictEqual(str, expected);
      done();
    });
  });

  test('g.toString() using promise API', function (done) {
    var expected = 'tinkergraph[vertices:6 edges:6]';
    g.toString()
      .then(function (str) { assert.strictEqual(str, expected); }, assert.ifError)
      .done(done);
  });

  test('g.toStringSync()', function (done) {
    var str = g.toStringSync();
    var expected = 'tinkergraph[vertices:6 edges:6]';
    assert.strictEqual(str, expected);
    done();
  });

  test('g.saveAndLoadGraphSON()', function (done) {
    tmp.tmpName(function (err, path) {
      if (err) {
        // A failure in tmpName is not a failure in gremlin-node.
        // If this ever fails, it is likely some environmental problem.
        throw err;
      }
      g.saveGraphSONSync(path);
      var tinker = gremlin.java.callStaticMethodSync('com.tinkerpop.gremlin.tinkergraph.structure.TinkerGraph', 'open');
      var h = gremlin.wrap(tinker);
      var str = h.toStringSync();
      var expected = 'tinkergraph[vertices:0 edges:0]';
      assert.strictEqual(str, expected);
      h.loadGraphSONSync(path);
      str = h.toStringSync();
      expected = 'tinkergraph[vertices:6 edges:6]';
      assert.strictEqual(str, expected);
      fs.unlink(path, done);
    });
  });

});
