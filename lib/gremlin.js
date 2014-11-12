'use strict';

var _ = require('lodash');
var fs = require('fs');
var glob = require('glob');
var path = require('path');
var Q = require('q');

var GraphWrapper = require('./graph-wrapper');
var QueryWrapper = require('./query-wrapper');
var TraversalWrapper = require('./traversal-wrapper');
var VertexWrapper = require('./vertex-wrapper');
var EdgeWrapper = require('./edge-wrapper');

var Gremlin = module.exports = function (opts) {
  opts = opts || {};
  opts.options = opts.options || [];
  opts.classpath = opts.classpath || [];

  // add default globbed lib/**/*.jar classpath
  opts.classpath.push(path.join(__dirname, '..', 'target', '**', '*.jar'));

  // initialize java
  var java = this.java = require('java');

  // add options
  java.options.push('-Djava.awt.headless=true');
  for (var i = 0; i < opts.options.length; i++) {
    java.options.push(opts.options[i]);
  }

  // add jar files
  for (var i = 0; i < opts.classpath.length; i++) {
    var pattern = opts.classpath[i];
    var filenames = glob.sync(pattern);
    for (var j = 0; j < filenames.length; j++) {
      java.classpath.push(filenames[j]);
    }
  }

  var MIN_VALUE = 0;
  var MAX_VALUE = java.newInstanceSync('java.lang.Long', 2147483647);

  this.GremlinTraversal = java.import('com.tinkerpop.gremlin.process.graph.GraphTraversal');    // ##TP3##

  this.NULL = java.callStaticMethodSync('org.codehaus.groovy.runtime.NullObject', 'getNullObject');

  var Class = this.Class = java.import('java.lang.Class');
  this.ArrayList = java.import('java.util.ArrayList');
  this.HashMap = java.import('java.util.HashMap');
//   this.Table = java.import('com.tinkerpop.pipes.util.structures.Table');  // No TK3 equivalent?
//   this.Tree = java.import('com.tinkerpop.pipes.util.structures.Tree');   // TK2
  this.Tree = java.import('com.tinkerpop.gremlin.process.graph.util.Tree');   // TK3, but not used directly

  this.T = java.import('com.tinkerpop.gremlin.process.T');
  this.Direction = java.import('com.tinkerpop.gremlin.structure.Direction');
  this.Compare = java.import('com.tinkerpop.gremlin.structure.Compare');
  this.Contains = java.import('com.tinkerpop.gremlin.structure.Contains');
  this.ByteArrayOutputStream = java.import('java.io.ByteArrayOutputStream');
  this.UTF8 = java.import('java.nio.charset.StandardCharsets').UTF_8.nameSync();
  this.createGraphSONWriter = java.callStaticMethodSync('com.tinkerpop.gremlin.structure.io.graphson.GraphSONWriter', 'build').createSync;

  this.emptyArrayList = java.newArray('java.lang.String', []);

  this.ClassTypes = {
    'String': Class.forNameSync('java.lang.String'),
    'Vertex': java.getClassLoader().loadClassSync('com.tinkerpop.gremlin.structure.Vertex'),
    'Edge': java.getClassLoader().loadClassSync('com.tinkerpop.gremlin.structure.Edge'),
    'Byte': Class.forNameSync('java.lang.Byte'),
    'Character': Class.forNameSync('java.lang.Character'),
    'Double': Class.forNameSync('java.lang.Double'),
    'Float': Class.forNameSync('java.lang.Float'),
    'Integer': Class.forNameSync('java.lang.Integer'),
    'Long': Class.forNameSync('java.lang.Long'),
    'Short': Class.forNameSync('java.lang.Short'),
    'Number': Class.forNameSync('java.lang.Number'),
    'BigDecimal': Class.forNameSync('java.math.BigDecimal'),
    'BigInteger': Class.forNameSync('java.math.BigInteger')
  };
};

Gremlin.GraphWrapper = require('./graph-wrapper');
Gremlin.QueryWrapper = require('./query-wrapper');
Gremlin.TraversalWrapper = require('./traversal-wrapper');
Gremlin.ElementWrapper = require('./element-wrapper');
Gremlin.VertexWrapper = require('./vertex-wrapper');
Gremlin.EdgeWrapper = require('./edge-wrapper');

Gremlin.prototype.isType = function (o, typeName) {
  if (!o || !_.isObject(o)) return false;
  if (!o._isType) {
    o._isType = {};
  }
  var res = o._isType[typeName];
  if (res === undefined) {
    try {
      res = this.java.instanceOf(o, typeName);
    } catch (err) {
      res = false;
    }
    o._isType[typeName] = res;
  }
  return res;
};

Gremlin.prototype.toList = function (obj, callback) {
  var promise;
  if (_.isArray(obj)) {
    var list = new this.ArrayList();
    for (var i = 0; i < obj.length; i++) {
      list.addSync(obj[i]);
    }
    promise = new Q(list);
  }
  else if (obj.getClassSync().isArraySync()) {
    promise = Q.nbind(this.java.callStaticMethod, this.java)('java.util.Arrays', 'asList', obj);
  }
  else {
    promise = Q.nbind(this.java.callStaticMethod, this.java)('com.google.common.collect.Lists', 'newArrayList', obj);
  }

  return promise.nodeify(callback);
};

Gremlin.prototype.toListSync = function (obj) {
  if (_.isArray(obj)) {
    var list = new this.ArrayList();
    for (var i = 0; i < obj.length; i++) {
      list.addSync(obj[i]);
    }
    return list;
  }
  if (obj.getClassSync().isArraySync()) {
    return this.java.callStaticMethodSync('java.util.Arrays', 'asList', obj);
  }
  return this.java.callStaticMethodSync('com.google.common.collect.Lists', 'newArrayList', obj);
};

Gremlin.prototype.getEngine = function () {
  if (this._engine) {
    return this._engine;
  }
  var GremlinGroovyScriptEngine = this.java.import('com.tinkerpop.gremlin.groovy.jsr223.GremlinGroovyScriptEngine');
  this._engine = new GremlinGroovyScriptEngine();
  return this._engine;
};

Gremlin.prototype.wrap = function (val) {
  return new GraphWrapper(this, val);
};

Gremlin.prototype.wrapQuery = function (val) {
  return new QueryWrapper(this, val);
};

Gremlin.prototype.wrapTraversal = function (val) {
  return new TraversalWrapper(this, val);
};

Gremlin.prototype.wrapVertex = function (val) {
  return new VertexWrapper(this, val);
};

Gremlin.prototype.wrapEdge = function (val) {
  return new EdgeWrapper(this, val);
};

Gremlin.prototype.extractArguments = function (args) {
  var callback = _.last(args);

  if (_.isFunction(callback))
    args = _.initial(args);
  else
    callback = undefined;

  if (args.length === 1 && _.isArray(args[0]))
    args = args[0];

  return {
    args: args,
    callback: callback
  };
};

Gremlin.prototype.jsonStringify = function (obj, callback) {
  // This method returns a json formatted string (either via a promise or a callback)
  if (obj && obj.jsonStringify) {
    return obj.jsonStringify().nodeify(callback);
  }
  else if (_.isArray(obj)) {
    return this.arrayJsonStringify(obj).nodeify(callback); // returns a string '[ <json>, <json>, ... ]'
  }
  else {
    return new Q(JSON.stringify(obj)).nodeify(callback);
  }
};

Gremlin.prototype.jsonStringifySync = function (obj) {
  // This method returns a json formatted string (synchronously)
  if (obj && obj.jsonStringifySync) {
    return obj.jsonStringifySync();
  }
  else if (_.isArray(obj)) {
    return this.arrayJsonStringifySync(obj); // returns a string '[ <json>, <json>, ... ]'
  }
  else {
    return new Q(JSON.stringify(obj));
  }
};

Gremlin.prototype.arrayJsonStringify = function (arr, callback) {
  var self = this;
  var promises = _.map(arr, function (val) { return self.jsonStringify(val); });
  return Q.all(promises)
    .then(function (results) { return '[' + results.join() + ']'; })
    .nodeify(callback);
};

Gremlin.prototype.arrayJsonStringifySync = function (arr) {
  var self = this;
  var results = _.map(arr, function (val) { return self.jsonStringifySync(val); });
  return '[' + results.join() + ']';
};

Gremlin.prototype.normalizeObj = function (obj) {
  var self = this;
  if (_.isArray(obj)) {
    return _.map(obj, function (val) { return self.normalizeObj(val); });
  }
  else if (_.isString(obj) && obj === 'null') {
    return null;
  }
  else if (_.isString(obj) && obj === 'undefined') {
    return undefined;
  }
  else if (_.isPlainObject(obj)) {
    var norm = {};
    _.forOwn(obj, function (val, key) {
      if (!_.isEmpty(val) || _.indexOf(['hiddens', 'properties'], key) === -1) {
        norm[key] = self.normalizeObj(val);
      }
    });
    return norm;
  }
  else {
    return obj;
  }
};

Gremlin.prototype.toJSON = function (obj, callback) {
  var self = this;
  return self.jsonStringify(obj)
    .then(function (jsonStr) {
      if (_.isUndefined(jsonStr))
        return undefined;
      try {
        return self.normalizeObj(JSON.parse(jsonStr));
      }
      catch (err) {
        console.error('While parsing:', obj, jsonStr);
        console.error('Got error:', err);
        return undefined;
      }
    })
    .nodeify(callback);
};

Gremlin.prototype.toJSONSync = function (obj, callback) {
  var self = this;
  var jsonStr = self.jsonStringifySync(obj);
  if (_.isUndefined(jsonStr))
    return undefined;
  try {
    return self.normalizeObj(JSON.parse(jsonStr));
  }
  catch (err) {
    console.error('While parsing:', obj, jsonStr);
    console.error('Got error:', err);
    return undefined;
  }
};
