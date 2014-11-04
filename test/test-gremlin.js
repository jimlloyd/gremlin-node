'use strict';

var _ = require('lodash');
var assert = require('assert');
var sinon = require('sinon');
var Gremlin = require('../lib/gremlin');
var GraphWrapper = require('../lib/graph-wrapper');

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

  test('Wrapped objects can be converted to JS objects using gremlin.toJsObj', function (done) {
    g.v(2, function (err, res) {
      assert.ifError(err);
      gremlin.toJsObj(res, function (err, json) {
        assert.ifError(err);
        // console.log(require('util').inspect(json, {depth: null}));
        assert(json.id === 2);
        done();
      });
    });
  });

  test('gremlin.toJsObj returns null when passed null', function (done) {
    gremlin.toJsObj(null, function (err, json) {
      assert.ifError(err);
      assert.strictEqual(json, null);
      done();
    });
  });

  test('gremlin.toJSON throws error but does not crash when passed undefined', function (done) {
    gremlin.toJsObj(undefined, function (err, json) {
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

});
