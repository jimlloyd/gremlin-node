'use strict';

var _ = require('lodash');
var assert = require('assert');
var Gremlin = require('../lib/gremlin');
var GraphWrapper = require('../lib/graph-wrapper');
var EdgeWrapper = require('../lib/edge-wrapper');

suite('gremlin', function () {
  var gremlin;
  var graph;
  var g;

  suiteSetup(function () {
    gremlin = new Gremlin();
  });

  setup(function () {
    var TinkerGraphFactory = gremlin.java.import('com.tinkerpop.gremlin.tinkergraph.structure.TinkerFactory');
    graph = TinkerGraphFactory.createClassicSync();
    g = new GraphWrapper(gremlin, graph);
  });

  test('gremlin.toList(jsarray) using callback API', function (done) {
    gremlin.toList(['a', 'b', 'c'], function (err, list) {
      assert.ifError(err);
      assert(gremlin.isType(list, 'java.util.Collection'));
      done();
    });
  });

  test('gremlin.toList(jsarray) using promise API', function (done) {
    gremlin.toList(['a', 'b', 'c'])
      .then(function (list) { assert(gremlin.isType(list, 'java.util.Collection')); }, assert.ifError)
      .done(done);
  });

  test('gremlin.forEach() ArrayList', function (done) {
    var list = gremlin.toListSync(['a', 'b', 'c']);
    var iterator = list.listIteratorSync();
    var count = 0;
    gremlin.forEach(iterator, function (elem) {
      assert.ok(_.isString(elem));
      ++count;
    })
    .then(function () {
      assert.strictEqual(count, 3);
    })
    .done(done);
  });

  test('gremlin.forEach() Traversal', function (done) {
    var iterator = g.E();
    var count = 0;
    gremlin.forEach(iterator, function (elem) {
      assert.ok(elem instanceof EdgeWrapper);
      ++count;
    })
    .then(function () {
      assert.strictEqual(count, 6);
    })
    .done(done);
  });

  test('newGroovyLambda', function () {
    var lambda = gremlin.newGroovyLambda('{ a -> a < 100 }');
    assert.equal(lambda.applySync(0), true);
    assert.equal(lambda.applySync(99), true);
    assert.equal(lambda.applySync(100), false);
  });

  test('importGroovy', function () {
    // We're going to try to define a closure that references an application-specific datatype.
    var groovy = '{ -> new TestClass() }';

    // First check that TestClass is not defined.
    assert.throws(function () { gremlin.newGroovyLambda(groovy); }, /unable to resolve class TestClass/);

    // Now define it, and try again.
    gremlin.importGroovy('com.entrendipity.gremlinnode.testing.TestClass');
    var lambda = gremlin.newGroovyLambda(groovy);
    assert.equal(lambda.getSync().toString(), "TestClass");
  });

  test('newJavaScriptLambda', function () {
    var lambda = gremlin.newJavaScriptLambda(
      'a.split(",").map(function (x) { return (Number(x) < 100).toString(); }).join(", ")');
    assert.equal(lambda.applySync('0, 99, 100'), 'true, true, false');
  });

});
