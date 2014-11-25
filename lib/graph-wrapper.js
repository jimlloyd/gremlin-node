'use strict';

var _ = require('lodash');
var VertexWrapper = require('./vertex-wrapper');
var EdgeWrapper = require('./edge-wrapper');
var Q = require('q');

var assert = require('assert');   // TODO: remove asserts

var GraphWrapper = module.exports = function (gremlin, graph) {
  assert.ok(gremlin);
  assert.ok(graph);
  this.gremlin = gremlin;
  this.graph = graph;

  // re-export some of gremlin's utility functions as
  // part of the graph wrapper instance
  this.java = gremlin.java;
  this.ClassTypes = gremlin.ClassTypes;
  this.Tokens = gremlin.Tokens;
  this.Compare = gremlin.Compare;
  this.Contains = gremlin.Contains;
  this.Direction = gremlin.Direction;
  this.ArrayList = gremlin.ArrayList;
  this.HashMap = gremlin.HashMap;
//   this.Table = gremlin.Table;    // No TK3 equivalent?
  this.Tree = gremlin.Tree;
  this.isType = gremlin.isType.bind(gremlin);
  this.toList = gremlin.toList.bind(gremlin);
  this.toListSync = gremlin.toListSync.bind(gremlin);

  this.toJSON = function (callback) {
    return Q.nbind(this.gremlin.toJSON, this.gremlin)(this.graph)
      .nodeify(callback);
  };

  this.toJSONSync = function () {
    return this.gremlin.toJSONSync(this.graph);
  };
};

GraphWrapper.prototype.loadGraphSONSync = function (filename) {
  var FileInputStream = this.java.import('java.io.FileInputStream');
  var stream = new FileInputStream(filename);
  var builder = this.java.callStaticMethodSync('com.tinkerpop.gremlin.structure.io.graphson.GraphSONReader', 'build');
  var reader = builder.createSync();
  reader.readGraphSync(stream, this.graph);
};

GraphWrapper.prototype.saveGraphSONSync = function (filename) {
  var FileOutputStream = this.java.import('java.io.FileOutputStream');
  var stream = new FileOutputStream(filename);
  var builder = this.java.callStaticMethodSync('com.tinkerpop.gremlin.structure.io.graphson.GraphSONWriter', 'build');
  var writer = builder.createSync();
  writer.writeGraphSync(stream, this.graph);
};

GraphWrapper.prototype._supportsTransactions = function () {
  var features = this.graph.featuresSync();
  var graphFeatures = features.graphSync();
  return graphFeatures.supportsThreadedTransactionsSync();
};

GraphWrapper.prototype._getTransaction = function () {
  // Transactions in TransactionalGraph's are often, by default, bound against the
  // executing thread (e.g. as a ThreadLocal variable). This behavior is not very
  // helpful in JavaScript because while the main execution is in fact performed
  // on a single thread, often a pool of threads exist to service asynchronous tasks,
  // making our tasks often operate on an incorrect transaction instance.
  //
  // Due to this, we try and avoid this default thread-bound functionality and manage
  // our own life-cycle if the supplied graph instance provides the interface to create
  // a transaction independent of the executing thread.
  //
  if (this.graph.txn) {
    return this.graph.txn;
  }

  var supportsTransactions = this._supportsTransactions();
  if (!supportsTransactions) {
    // When transactions are not supported, we return the graph itself.
    return this.graph;
  }

  // TODO: The following introduces a new member this.graph.tx, which seems to be new with TK3.
  // Given the tx, we can create a Graph interface that is a transaction that can be executed
  // across multiple threads. Other methods involving transactions almost certainly need to
  // be updated.
  this.graph.tx = this.graph.txSync();
  this.graph.txn = this.graph.tx.createSync();
  return this.graph.txn;
};

GraphWrapper.prototype._clearTransaction = function () {
  if (this.graph.txn) {
    this.graph.tx = null;
    this.graph.txn = null;
  }
};

// com.tinkerpop.blueprints.Graph interface
GraphWrapper.prototype.addVertex = function (properties, callback) {
  var gremlin = this.gremlin;
  var txn = this._getTransaction();

  // In TP2, the first argument as an id, which was often specified as null.
  // In TP3, the first argument is an simple object with initial properties.
  // These asserts should help catch code that needs to be updated for TP3.
  assert(_.isObject(properties));
  assert(!_.isFunction(properties));

  var deferred = Q.defer();

  txn.addVertex(gremlin.propertiesToVarArgs(properties), function (err, v) {
    if (err)
      return deferred.reject(err);
    else
      return deferred.resolve(gremlin.wrapVertex(v));
  });

  return deferred.promise.nodeify(callback);
};

GraphWrapper.prototype.getVertex = function (id, callback) {
  var gremlin = this.gremlin;
  var txn = this._getTransaction();

  return Q.nbind(txn.v, txn)(id)
    .then(function (v) { return new Q(v ? gremlin.wrapVertex(v) : null); })
    .nodeify(callback);
};

// In Tinkerpop3, instead of graph.removeVertex(v), just do v.remove().
// GraphWrapper.prototype.removeVertex = function (vertex, callback) {
//   var txn = this._getTransaction();
//
//   if (!(vertex instanceof VertexWrapper)) {
//     throw new TypeError('vertex must be an instance of VertexWrapper');
//   }
//
//   return Q.nbind(txn.removeVertex, txn)(vertex.unwrap())
//     .nodeify(callback);
// };

GraphWrapper.prototype.addEdge = function (id, outVertex, inVertex, label, callback) {
  var gremlin = this.gremlin;
  var txn = this._getTransaction();

  if (!(outVertex instanceof VertexWrapper)) {
    throw new TypeError('outVertex must be an instance of VertexWrapper');
  }
  if (!(inVertex instanceof VertexWrapper)) {
    throw new TypeError('inVertex must be an instance of VertexWrapper');
  }

  return Q.nbind(txn.addEdge, txn)(id, outVertex.unwrap(), inVertex.unwrap(), label)
    .then(function (e) { return new Q(gremlin.wrapEdge(e)); })
    .nodeify(callback);
};

GraphWrapper.prototype.getEdge = function (id, callback) {
  var gremlin = this.gremlin;
  var txn = this._getTransaction();

  return Q.nbind(txn.e, txn)(id)
    .then(function (e) { return new Q(e ? gremlin.wrapEdge(e) : null); })
    .nodeify(callback);
};

GraphWrapper.prototype.removeEdge = function (edge, callback) {
  var txn = this._getTransaction();

  if (!(edge instanceof EdgeWrapper)) {
    throw new TypeError('edge must be an instance of EdgeWrapper');
  }

  return Q.nbind(txn.removeEdge, txn)(edge.unwrap())
    .nodeify(callback);
};

GraphWrapper.prototype.query = function () {
  var txn = this._getTransaction();
  return this.gremlin.wrapQuery(txn.querySync());
};

GraphWrapper.prototype.requiresThreadedTransactions = function () {
  var supportsTransactions = this._supportsTransactions();
  if (!this._supportsTransactions()) {
    throw new Error('Graph instance must support threaded transactions.');
  }
};

GraphWrapper.prototype.newTransaction = function () {
  this.requiresThreadedTransactions();
  var txn = this.graph.txSync();
  return this.gremlin.wrap(txn);
};

GraphWrapper.prototype.commit = function (callback) {
  this.requiresThreadedTransactions();
  var txn = this._getTransaction();
  this._clearTransaction();
  txn.commit(callback);
};

GraphWrapper.prototype.rollback = function (callback) {
  this.requiresThreadedTransactions();
  var txn = this._getTransaction();
  this._clearTransaction();
  txn.rollback(callback);
};

GraphWrapper.prototype.shutdown = function (callback) {
  this.requiresThreadedTransactions();
  var txn = this._getTransaction();
  this._clearTransaction();
  txn.shutdown(callback);
};

// gremlin shell extensions for the graph object
GraphWrapper.prototype._ = function () {
  var txn = this._getTransaction();
  var traversal = this.gremlin.wrapTraversal(txn);
  traversal.traversal._Sync();
  return traversal;
};

GraphWrapper.prototype.start = function (start) {
  var txn = this._getTransaction();
  var traversal = this.gremlin.wrapTraversal(txn);
  // conditionally unwrap, we may be being passed a Java list instead
  // of one of our wrapper JavaScript objects
  if (start.unwrap) {
    start = start.unwrap();
  }
  return traversal.start(start);
};

// TODO: In TK2, we could say g.V(key, value).
// In TK3, we have to say g.V().has(key, value).
// We could restore the behavior by checking if args.length>0, and automatically
// adding the has step.
GraphWrapper.prototype.V = function () {
  var args = Array.prototype.slice.call(arguments);
  assert(args.length === 0);
  var txn = this._getTransaction();
  return this.gremlin.wrapTraversal(txn.VSync());
};

// TODO: Likewise, g.E(key, value) is currently disallowed, but could be supported.
// See above for g.V()
GraphWrapper.prototype.E = function () {
  var args = Array.prototype.slice.call(arguments);
  assert(args.length === 0);
  var txn = this._getTransaction();
  return this.gremlin.wrapTraversal(txn.ESync());
};

GraphWrapper.prototype.v = function (id, callback) {
  var txn = this._getTransaction();
  var gremlin = this.gremlin;

  return Q.nbind(txn.v, txn)(id)
    .then(function (v) { return gremlin.wrapVertex(v); })
    .catch(function (err) {
      if (err.toString().match(/java.util.NoSuchElementException/))
        return undefined;
      else
        throw err;
    })
    .nodeify(callback);
};

GraphWrapper.prototype.e = function () {
  var txn = this._getTransaction();
  var gremlin = this.gremlin;
  var argPair = gremlin.extractArguments(Array.prototype.slice.call(arguments));
  if (argPair.args.length === 0)
    throw new Error('e() requires at least one argument.');

  return Q.all(argPair.args.map(function (id) {
      return Q.nbind(txn.getEdge, txn)(id);
    }))
    .then(function (edges) {
      var list = new gremlin.ArrayList();
      edges.forEach(function (e) {
        list.addSync(e);
      });
      return new Q(list);
    })
    .then(function (list) {
      return new Q(gremlin.wrapTraversal(list.iteratorSync()));
    })
    .nodeify(argPair.callback);
};

GraphWrapper.prototype.toString = function (callback) {
  return Q.nbind(this.graph.toString, this.graph)().nodeify(callback);
};

GraphWrapper.prototype.toStringSync = function () {
  return this.graph.toStringSync();
};
