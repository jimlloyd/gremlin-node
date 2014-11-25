'use strict';

var _ = require('lodash');
var Q = require('q');
var dlog = require('debug')('traversal-wrapper');

var assert = require('assert'); // TODO: remove

var TraversalWrapper = module.exports = function (gremlin, traversal) {
  assert.ok(traversal);
  assert.ok(gremlin.isType(traversal, 'com.tinkerpop.gremlin.process.Traversal'));
  this.gremlin = gremlin;
  this.traversal = traversal;
};

TraversalWrapper.prototype._parseVarargs = function (args, type) {
  var va, self = this;
  if (_.isArray(args[args.length - 1])) {
    va = args.pop();
  } else {
    va = [];
    // HACK - instead of actually converting JS strings -> java.lang.String
    // instances as part of javify, we check the type with _isString
    var test = type === 'java.lang.String' ? _.isString : function (o) {
      return self.gremlin.isType(o, type);
    };
    while (test(args[args.length - 1])) {
      va.unshift(args.pop());
    }
  }
  args.push(this.gremlin.java.newArray(type, va));
};

TraversalWrapper.prototype._isClosure = function (val) {
  var closureRegex = /^\{.*\}$/;
  return _.isString(val) && val.search(closureRegex) > -1;
};

TraversalWrapper.prototype._javify = function (arg) {
  if (arg.unwrap) {
    return arg.unwrap();
  } else if (this._isClosure(arg)) {
    // TK3 TODO: In TK2, there was simply closures. In TK3, there are typed predicates that can wrap closures.
    // Ideally we could just do something like the following. But GPredicate is only appopriate in some cases.
    // We probably have to dispatch correctly for six different function types defined in package
    // com.tinkerpop.gremlin.groovy.function, i.e. see
    // http://www.tinkerpop.com/javadocs/3.0.0.M4/full/com/tinkerpop/gremlin/groovy/function/package-summary.html
    var closure = this.gremlin.getEngine().evalSync(arg);
    var GPredicate = this.gremlin.java.import('com.tinkerpop.gremlin.groovy.function.GPredicate');
    var predicate = new GPredicate(closure);
    return predicate;
  }
  return arg;
};

TraversalWrapper.prototype._jsify = function (arg) {
  if (!_.isObject(arg)) {
    return arg;
  }

  if (!arg._isType) {
    arg._isType = {};
  }

  if (arg.longValue) {
    arg._isType.longValue = true;
    return parseInt(arg.longValue, 10);
  } else if (this.gremlin.isType(arg, 'com.tinkerpop.gremlin.structure.Vertex')) {
    return this.gremlin.wrapVertex(arg);
  } else if (this.gremlin.isType(arg, 'com.tinkerpop.gremlin.structure.Edge')) {
    return this.gremlin.wrapEdge(arg);
  } else if (this.gremlin.isType(arg, 'java.util.List')) {
    return arg;
  } else if (this.gremlin.isType(arg, 'java.util.Map')) {
    // it seems this type of coercion could be ported to node-java
    // https://github.com/joeferner/node-java/issues/56
    var map = {};
    var it = arg.entrySetSync().iteratorSync();
    while (it.hasNextSync()) {
      var pair = it.nextSync();
      map[pair.getKeySync()] = this._jsify(pair.getValueSync());
    }
    return map;
  } else if (this.gremlin.isType(arg, 'com.tinkerpop.gremlin.process.util.BulkSet')) {
    var arr = [];
    var it = arg.iteratorSync();
    while (it.hasNextSync()) {
      var key = it.nextSync();
      var count = this._jsify(arg.getSync(key));
      var obj = {
        key: this._jsify(key),
        count: count
      };
      arr.push(obj);
    }
    return arr;
  } else if (this.gremlin.isType(arg, 'java.util.Object')) {
    return arg;
  }
  return arg;
};

TraversalWrapper.prototype.unwrap = function () {
  return this.traversal;
};

TraversalWrapper.prototype.add = function (type, args) {
  this.traversal[type + 'Sync'].apply(this.traversal, args);
  return this;
};

TraversalWrapper.prototype.V = function () {
  var args = Array.prototype.slice.call(arguments);
  return this.add('V', args);
};

TraversalWrapper.prototype.E = function () {
  var args = Array.prototype.slice.call(arguments);
  return this.add('E', args);
};

TraversalWrapper.prototype.has = function () {
  var args = Array.prototype.slice.call(arguments);
  return this.add('has', args);
};

TraversalWrapper.prototype.hasNot = function () {
  var args = Array.prototype.slice.call(arguments);
  return this.add('hasNot', args);
};

TraversalWrapper.prototype.interval = function () {
  var args = Array.prototype.slice.call(arguments);
  return this.add('interval', args);
};

TraversalWrapper.prototype.bothE = function () {
  var args = Array.prototype.slice.call(arguments);
  this._parseVarargs(args, 'java.lang.String');
  return this.add('bothE', args);
};

TraversalWrapper.prototype.both = function () {
  var args = Array.prototype.slice.call(arguments);
  this._parseVarargs(args, 'java.lang.String');
  return this.add('both', args);
};

TraversalWrapper.prototype.bothV = function () {
  return this.add('bothV');
};

TraversalWrapper.prototype.idEdge = function () {
  var args = Array.prototype.slice.call(arguments);
  return this.add('idEdge', args);
};

TraversalWrapper.prototype.id = function () {
  return this.add('id');
};

TraversalWrapper.prototype.idVertex = function () {
  var args = Array.prototype.slice.call(arguments);
  return this.add('idVertex', args);
};

TraversalWrapper.prototype.inE = function () {
  var args = Array.prototype.slice.call(arguments);
  this._parseVarargs(args, 'java.lang.String');
  return this.add('inE', args);
};

TraversalWrapper.prototype.in = function () {
  var args = Array.prototype.slice.call(arguments);
  this._parseVarargs(args, 'java.lang.String');
  return this.add('in', args);
};

TraversalWrapper.prototype.addInE = function (edgeLabel, stepLabel, props) {
  this.traversal.addInESync(edgeLabel, stepLabel, this.gremlin.propertiesToVarArgs(props));
  return this;
};

TraversalWrapper.prototype.addOutE = function (edgeLabel, stepLabel, props) {
  this.traversal.addOutESync(edgeLabel, stepLabel, this.gremlin.propertiesToVarArgs(props));
  return this;
};

TraversalWrapper.prototype.addBothE = function (edgeLabel, stepLabel, props) {
  this.traversal.addBothESync(edgeLabel, stepLabel, this.gremlin.propertiesToVarArgs(props));
  return this;
};

TraversalWrapper.prototype.addE = function (direction, edgeLabel, stepLabel, props) {
  this.traversal.addESync(direction, edgeLabel, stepLabel, this.gremlin.propertiesToVarArgs(props));
  return this;
};

TraversalWrapper.prototype.inV = function () {
  return this.add('inV');
};

TraversalWrapper.prototype.label = function () {
  return this.add('label');
};

TraversalWrapper.prototype.outE = function () {
  var args = Array.prototype.slice.call(arguments);
  this._parseVarargs(args, 'java.lang.String');
  return this.add('outE', args);
};

TraversalWrapper.prototype.out = function () {
  var args = Array.prototype.slice.call(arguments);
  this._parseVarargs(args, 'java.lang.String');
  return this.add('out', args);
};

TraversalWrapper.prototype.outV = function () {
  return this.add('outV');
};

TraversalWrapper.prototype.map = function () {
  var args = Array.prototype.slice.call(arguments);
  this._parseVarargs(args, 'java.lang.String');
  return this.add('map', args);
};

TraversalWrapper.prototype.value = function () {
  var args = Array.prototype.slice.call(arguments);
  return this.add('value', args);
};

TraversalWrapper.prototype.values = function () {
  var args = Array.prototype.slice.call(arguments);
  this._parseVarargs(args, 'java.lang.String');
  return this.add('values', args);
};

TraversalWrapper.prototype.step = function () {
  var args = Array.prototype.slice.call(arguments).map(this._javify.bind(this));
  return this.add('step', args);
};

/////////////////////////
/// BRANCH TRAVERSALS ///
/////////////////////////

TraversalWrapper.prototype.copySplit = function () {  // TK2 only?
  var args = Array.prototype.slice.call(arguments).map(this._javify.bind(this));
  this._parseVarargs(args, 'com.tinkerpop.gremlin.process.Traversal');
  return this.add('copySplit', args);
};

TraversalWrapper.prototype.exhaustMerge = function () {
  return this.add('exhaustMerge');
};

TraversalWrapper.prototype.fairMerge = function () {
  return this.add('fairMerge');
};

TraversalWrapper.prototype.ifThenElse = function () {
  var args = Array.prototype.slice.call(arguments).map(this._javify.bind(this));
  return this.add('ifThenElse', args);
};

TraversalWrapper.prototype.jump = function () {
  var args = Array.prototype.slice.call(arguments).map(this._javify.bind(this));
  return this.add('jump', args);
};

/////////////////////////
/// FILTER TRAVERSALS ///
/////////////////////////

TraversalWrapper.prototype.and = function (/*final Traversal<E, ?>... traversals*/) {
  var args = Array.prototype.slice.call(arguments).map(this._javify.bind(this));
  this._parseVarargs(args, 'com.tinkerpop.gremlin.process.Traversal');
  return this.add('and', args);
};

TraversalWrapper.prototype.back = function (step) {
  var args = Array.prototype.slice.call(arguments);
  return this.add('back', args);
};

TraversalWrapper.prototype.dedup = function (closure) {
  var args = Array.prototype.slice.call(arguments).map(this._javify.bind(this));
  return this.add('dedup', args);
};

TraversalWrapper.prototype.except = function () {
  var args = Array.prototype.slice.call(arguments);
  if (this.gremlin.isType(args[0], 'java.util.Collection')) {
    // assume except(final Collection<E> collection)
  } else if (_.isArray(args[0])) {
    // assume except(final Collection<E> collection)
    args[0] = this.gremlin.toListSync(args[0]);
  } else {
    // assume except(final String... namedSteps)
    this._parseVarargs(args, 'java.lang.String');
  }
  return this.add('except', args);
};

TraversalWrapper.prototype.filter = function (closure) {
  var args = Array.prototype.slice.call(arguments).map(this._javify.bind(this));
  return this.add('filter', args);
};

TraversalWrapper.prototype.or = function (/*final Traversal<E, ?>... traversals*/) {
  var args = Array.prototype.slice.call(arguments).map(this._javify.bind(this));
  this._parseVarargs(args, 'com.tinkerpop.gremlin.process.Traversal');
  return this.add('or', args);
};

TraversalWrapper.prototype.random = function () {
  var args = Array.prototype.slice.call(arguments);
  return this.add('random', args);
};

TraversalWrapper.prototype.index = function (idx) {
  return this.add('range', [idx, idx]);
};

TraversalWrapper.prototype.range = function () {
  var args = Array.prototype.slice.call(arguments);
  return this.add('range', args);
};

TraversalWrapper.prototype.retain = function (/*final Collection<E> collection*/) {
  var args = Array.prototype.slice.call(arguments);
  if (this.gremlin.isType(args[0], 'java.util.Collection')) {
    // assume retain(final Collection<E> collection)
  } else if (_.isArray(args[0])) {
    // assume retain(final Collection<E> collection)
    args[0] = this.gremlin.toListSync(args[0]);
  } else {
    // assume retain(final String... namedSteps)
    this._parseVarargs(args, 'java.lang.String');
  }
  return this.add('retain', args);
};

TraversalWrapper.prototype.simplePath = function () {
  return this.add('simplePath');
};

//////////////////////////////
/// SIDE-EFFECT TRAVERSALS ///
//////////////////////////////

// TraversalWrapper.prototype.aggregate = function () {
//   var args = Array.prototype.slice.call(arguments).map(this._javify.bind(this));
//   if (_.isArray(args[0])) {
//     args[0] = this.gremlin.toListSync(args[0]);
//   }
//   return this.add('aggregate', args);
// };

TraversalWrapper.prototype.aggregate = function () {
  var args = Array.prototype.slice.call(arguments);
  return this.add('aggregate', args);
};

TraversalWrapper.prototype.optional = function () {
  var args = Array.prototype.slice.call(arguments);
  return this.add('optional', args);
};

TraversalWrapper.prototype.groupBy = function (map, closure) {
  var args = Array.prototype.slice.call(arguments).map(this._javify.bind(this));
  return this.add('groupBy', args);
};

TraversalWrapper.prototype.groupCount = function () {
  var args = Array.prototype.slice.call(arguments).map(this._javify.bind(this));
  return this.add('groupCount', args);
};

TraversalWrapper.prototype.linkOut = function () {
  var args = Array.prototype.slice.call(arguments);
  return this.add('linkOut', args);
};

TraversalWrapper.prototype.linkIn = function () {
  var args = Array.prototype.slice.call(arguments);
  return this.add('linkIn', args);
};

TraversalWrapper.prototype.linkBoth = function () {
  var args = Array.prototype.slice.call(arguments);
  return this.add('linkBoth', args);
};

TraversalWrapper.prototype.sideEffect = function () {
  var args = Array.prototype.slice.call(arguments).map(this._javify.bind(this));
  return this.add('sideEffect', args);
};

TraversalWrapper.prototype.store = function () {
  var args = Array.prototype.slice.call(arguments).map(this._javify.bind(this));
  if (_.isArray(args[0])) {
    args[0] = this.gremlin.toListSync(args[0]);
  }
  return this.add('store', args);
};

TraversalWrapper.prototype.table = function () {
  var args = Array.prototype.slice.call(arguments).map(this._javify.bind(this));
  this._parseVarargs(args, 'groovy.lang.Closure');
  if (_.isArray(args[1])) {
    args[1] = this.gremlin.toListSync(args[1]);
  }
  return this.add('table', args);
};

TraversalWrapper.prototype.tree = function () {
  var args = Array.prototype.slice.call(arguments).map(this._javify.bind(this));
  this._parseVarargs(args, 'groovy.lang.Closure');
  return this.add('tree', args);
};

////////////////////////////
/// TRANSFORM TRAVERSALS ///
////////////////////////////

TraversalWrapper.prototype.gather = function () {
  var args = Array.prototype.slice.call(arguments).map(this._javify.bind(this));
  return this.add('gather', args);
};

TraversalWrapper.prototype._ = function () {
  return this.add('_');
};

TraversalWrapper.prototype.memoize = function () {
  var args = Array.prototype.slice.call(arguments);
  return this.add('memoize', args);
};

TraversalWrapper.prototype.order = function () {
  var args = Array.prototype.slice.call(arguments).map(this._javify.bind(this));
  return this.add('order', args);
};

TraversalWrapper.prototype.path = function () {
  var args = Array.prototype.slice.call(arguments).map(this._javify.bind(this));
  this._parseVarargs(args, 'groovy.lang.Closure');
  return this.add('path', args);
};

TraversalWrapper.prototype.scatter = function () {
  return this.add('scatter');
};

TraversalWrapper.prototype.select = function () {
  var args = Array.prototype.slice.call(arguments).map(this._javify.bind(this));
  this._parseVarargs(args, 'groovy.lang.Closure');
  if (_.isArray(args[0])) {
    args[0] = this.gremlin.toListSync(args[0]);
  }
  return this.add('select', args);
};

TraversalWrapper.prototype.shuffle = function () {
  return this.add('shuffle');
};

TraversalWrapper.prototype.cap = function () {
  var args = Array.prototype.slice.call(arguments);
  return this.add('cap', args);
};

TraversalWrapper.prototype.count = function () {
  return this.add('count');
};

TraversalWrapper.prototype.orderMap = function () {
  var args = Array.prototype.slice.call(arguments).map(this._javify.bind(this));
  return this.add('orderMap', args);
};

TraversalWrapper.prototype.transform = function () {
  var args = Array.prototype.slice.call(arguments).map(this._javify.bind(this));
  return this.add('transform', args);
};

//////////////////////////
/// UTILITY TRAVERSALS ///
//////////////////////////

TraversalWrapper.prototype.as = function () {
  var args = Array.prototype.slice.call(arguments);
  return this.add('as', args);
};

TraversalWrapper.prototype.start = function (obj) {
  if (this.gremlin.isType(obj, 'java.lang.Iterable')) {
    throw new Error('Resolve iterable instances asynchronously to iterators to avoid unexpected potential blocking (e.g. it.iterator())');
  }
  return this.add('start', [obj]);
};

///////////////////////
/// UTILITY METHODS ///
///////////////////////

function traversalPromiseWrap(op) {
  return function () {
    var argPair = this.gremlin.extractArguments(Array.prototype.slice.call(arguments));
    dlog('traversalPromiseWrap(%s)', op, argPair);
    return Q.npost(this.traversal, op, argPair.args).nodeify(argPair.callback);
  };
}

function traversalPromiseJsifyWrap(op) {
  return function () {
    var self = this;
    var argPair = this.gremlin.extractArguments(Array.prototype.slice.call(arguments));
    dlog('traversalPromiseJsifyWrap(%s)', op, argPair, this.traversal[op]);
    return Q.npost(this.traversal, op, argPair.args)
      .then(function (res) {
        var jres = self._jsify(res);
        dlog('traversalPromiseJsifyWrap(%s)(result) = ', res, jres);
        return jres;
      })
      .nodeify(argPair.callback);
  };
}

// TraversalWrapper.prototype.count = traversalPromiseJsifyWrap('count');

TraversalWrapper.prototype.iterate = traversalPromiseWrap('iterate');
TraversalWrapper.prototype.iterator = traversalPromiseWrap('iterator');
TraversalWrapper.prototype.hasNext = traversalPromiseWrap('hasNext');
TraversalWrapper.prototype.next = traversalPromiseJsifyWrap('next');
TraversalWrapper.prototype.fill = traversalPromiseWrap('fill');
TraversalWrapper.prototype.enablePath = traversalPromiseWrap('enablePath');
TraversalWrapper.prototype.optimize = traversalPromiseWrap('optimize');
TraversalWrapper.prototype.remove = traversalPromiseWrap('remove');
TraversalWrapper.prototype.reset = traversalPromiseWrap('reset');
TraversalWrapper.prototype.getCurrentPath = traversalPromiseWrap('getCurrentPath');
TraversalWrapper.prototype.getStarts = traversalPromiseWrap('getStarts');
TraversalWrapper.prototype.get = traversalPromiseWrap('get');
TraversalWrapper.prototype.equals = traversalPromiseWrap('equals');
TraversalWrapper.prototype.size = traversalPromiseWrap('size');
TraversalWrapper.prototype.toList = traversalPromiseWrap('toList');

TraversalWrapper.prototype.toArray = function (callback) {
  var self = this;

  return self.toList()
    .then(function (list) {
      var arr = [];
      for (var i = 0, l = list.sizeSync(); i < l; i++) {
        var it = list.getSync(i);
        arr.push(self._jsify(it));
      }
      dlog('TraversalWrapper.prototype.toArray:', arr.length, arr);
      return arr;
    })
    .nodeify(callback);
};

TraversalWrapper.prototype.toArraySync = function () {
  var self = this;
  var list = self.traversal.toListSync();
  var arr = [];
  for (var i = 0, l = list.sizeSync(); i < l; i++) {
    var it = list.getSync(i);
    arr.push(self._jsify(it));
  }
  dlog('TraversalWrapper.prototype.toArraySync:', arr.length, arr);
  return arr;
};

TraversalWrapper.prototype.toJSON = function (callback) {
  var self = this;
  return self.toArray()
    .then(function (arr) {
      dlog('TraversalWrapper.prototype.toJSON:', arr);
      return self.gremlin.toJSON(arr);
    })
    .nodeify(callback);
};

TraversalWrapper.prototype.toJSONSync = function () {
  var self = this;
  var arr = self.toArraySync();
  return self.gremlin.toJSONSync(arr);
};
