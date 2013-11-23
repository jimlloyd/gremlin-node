'use strict';

var assert = require('assert');
var sinon = require('sinon');
var Gremlin = require('../lib/gremlin');
var GraphWrapper = require('../lib/graph-wrapper');

suite('graph-wrapper', function() {
  var gremlin;
  var graph;
  var g;
  var sandbox;

  suiteSetup(function() {
    gremlin = new Gremlin();
  });

  setup(function() {
    var TinkerGraphFactory = gremlin.java.import('com.tinkerpop.blueprints.impls.tg.TinkerGraphFactory');
    graph = TinkerGraphFactory.createTinkerGraphSync();
    g = new GraphWrapper(gremlin, graph);
    sandbox = sinon.sandbox.create();
  });

  teardown(function() {
    sandbox.restore();
  });

  test('Non ThreadedTransactionalGraph instances do not start unique transactions', function() {
    graph.newTransactionSync = sandbox.spy();
    g.addVertex(null);
    assert(!graph.newTransactionSync.called);
  });

  test('ThreadedTransactionalGraph starts unique transactions', function() {
    var fakeTxn = {
      addVertex: sandbox.stub()
    };
    var fakeGraph = {
      newTransactionSync: sandbox.stub().returns(fakeTxn)
    };
    var fakeGremlin = {
      isType: sandbox.stub()
        .withArgs(fakeGraph, 'com.tinkerpop.blueprints.ThreadedTransactionalGraph')
        .returns(true),
      toList: function () {},
      toListSync: function () {},
      toJSON: function () {},
      toJSONSync: function () {},
    };
    var g2 = new GraphWrapper(fakeGremlin, fakeGraph);

    // should start a new transaction
    g2.addVertex(null);

    // should re-use the existing transaction
    g2.addVertex(null);

    assert(fakeGremlin.isType.calledOnce);
    assert(fakeGraph.newTransactionSync.calledOnce);
    assert(fakeTxn.addVertex.calledTwice);
  });

  test('v(id) with single id', function (done) {
    g.v(2, function (err, data) {
      assert(!err && data.toString() === 'v[2]');
      done();
    });
  });

  test('v(id...) with id list', function (done) {
    g.v(2, 4, function (err, pipe) {
      pipe.toJSON(function (err, data) {
        assert(!err && data.length === 2);
        done();
      });
    });
  });

  test('v(id...) with id array', function (done) {
    g.v([2, 4], function (err, pipe) {
      pipe.toJSON(function (err, data) {
        assert(!err && data.length === 2);
        done();
      });
    });
  });

  test('v(id) with invalid id', function (done) {
    g.v(99, function (err, data) {
      assert(!err && data === null);
      done();
    });
  });
});
