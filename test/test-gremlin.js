'use strict';

var _ = require('lodash');
var assert = require('assert');
var sinon = require('sinon');
var Gremlin = require('../lib/gremlin');
var GraphWrapper = require('../lib/graph-wrapper');
var VertexWrapper = require('../lib/vertex-wrapper');
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

  test('Wrapped objects can be converted to JS objects using gremlin.toJSON', function (done) {
    g.v(2, function (err, v) {
      assert.ifError(err);
      assert(v instanceof VertexWrapper);
      gremlin.toJSON(v, function (err, json) {
        assert.ifError(err);
        assert.strictEqual(json.id, 2);
        done();
      });
    });
  });

  test('Wrapped objects can be converted to JS objects using gremlin.toJSONSync', function (done) {
    g.v(2, function (err, v) {
      assert.ifError(err);
      assert(v instanceof VertexWrapper);
      var json = gremlin.toJSONSync(v);
      assert.strictEqual(json.id, 2);
      done();
    });
  });

  test('gremlin.toJSON returns simple vertex by default', function (done) {
    g.v(2, function (err, v) {
      assert.ifError(err);
      assert(v instanceof VertexWrapper);
      gremlin.toJSON(v, function (err, json) {
        assert.ifError(err);
        var expected = {
          id: 2,
          label: 'vertex',
          type: 'vertex',
          properties: {name: 'vadas', age: 27}
        };
        assert.deepEqual(json, expected);
        done();
      });
    });
  });

  test('gremlin.toJSON(keepHiddens) returns full tinkerpop3 vertex', function (done) {
    g.v(2, function (err, v) {
      assert.ifError(err);
      assert(v instanceof VertexWrapper);
      gremlin.toJSON(v, { keepHiddens: true }, function (err, json) {
        assert.ifError(err);
        var expected = { id: 2,
          label: 'vertex',
          type: 'vertex',
          hiddens: {},
          properties: { name: 'vadas', age: 27 }
        };
        assert.deepEqual(json, expected);
        done();
      });
    });
  });

  test('gremlin.toJSON(strict) returns full tinkerpop3 vertex', function (done) {
    g.v(2, function (err, v) {
      assert.ifError(err);
      assert(v instanceof VertexWrapper);
      gremlin.toJSON(v, { strict: true }, function (err, json) {
        assert.ifError(err);
        var expected = { id: 2,
          label: 'vertex',
          type: 'vertex',
          hiddens: {},
          properties:
           { name:
              [ { id: 2,
                  label: 'name',
                  hiddens: {},
                  value: 'vadas',
                  properties: {} } ],
             age: [ { id: 3, label: 'age', hiddens: {}, value: 27, properties: {} } ] }
        };
        assert.deepEqual(json, expected);
        done();
      });
    });
  });

  test('gremlin.toJSON returns null when passed null', function (done) {
    gremlin.toJSON(null, function (err, json) {
      assert.ifError(err);
      assert.strictEqual(json, null);
      done();
    });
  });

  test('gremlin.toJSON returns undefined when passed undefined', function (done) {
    gremlin.toJSON(undefined, function (err, json) {
      assert.ifError(err);
      assert.strictEqual(json, undefined);
      done();
    });
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

  test('gremlin.forEach()', function (done) {
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

});
