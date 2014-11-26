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

Gremlin.prototype.jsonStringifySync = function (obj, options) {
  // This method returns a json formatted string (synchronously)
  if (obj && obj.jsonStringifySync) {
    return obj.jsonStringifySync(options);
  }
  else if (_.isArray(obj)) {
    return this.arrayJsonStringifySync(obj, options); // returns a string '[ <json>, <json>, ... ]'
  }
  else {
    return JSON.stringify(obj);
  }
};

Gremlin.prototype.arrayJsonStringifySync = function (arr, options) {
  var self = this;
  var results = _.map(arr, function (val) { return self.jsonStringifySync(val, options); });
  return '[' + results.join() + ']';
};

Gremlin.prototype.normalizeObj = function (obj, options) {
  var self = this;
  if (_.isArray(obj)) {
    return _.map(obj, function (val) { return self.normalizeObj(val, options); });
  }
  else if (_.isString(obj) && obj === 'null') {
    return null;
  }
  else if (_.isString(obj) && obj === 'undefined') {
    return undefined;
  }
  else if (_.isPlainObject(obj) && !options.keepHiddens) {
    var norm = {};
    _.forOwn(obj, function (val, key) {
      if (!_.isEmpty(val) || _.indexOf(['hiddens', 'properties'], key) === -1) {
        norm[key] = self.normalizeObj(val, options);
      }
    });
    return norm;
  }
  else {
    return obj;
  }
};

Gremlin.prototype.toJSON = function (obj, options, callback) {
  if (_.isUndefined(options))
    options = {};
  else if (_.isFunction(options) && _.isUndefined(callback)) {
    callback = options;
    options = {};
  }
  var self = this;
  return new Q(self.toJSONSync(obj, options)).nodeify(callback);
};

Gremlin.prototype.toJSONSync = function (obj, options) {
  if (_.isUndefined(obj) || _.isNull(obj))
    return obj;
  options = options || {};
  if (options.strict) {
    options.keepHiddens = true;
    options.fullVertexProperties = true;
  }
  var self = this;
  var jsonStr = self.jsonStringifySync(obj, options);
  if (_.isUndefined(jsonStr))
    return undefined;
  try {
    return self.normalizeObj(JSON.parse(jsonStr), options);
  }
  catch (err) {
    console.error('While parsing:', obj, jsonStr);
    console.error('Got error:', err);
    return undefined;
  }
};

// Various TP3 methods such as Graph.addVertex, Vertex.addEdge, and Traversal.addE take
// a final argument Object... propertyKeyValues. For Javacript it is convenient to pass
// a simple Javascript object { key1: value1, key2: value2, ... }.
// This function converts such an object to the java Object[] array.
// *props* is the Javascript object.
// *type* is a java class path, which defaults to java.lang.Object.
Gremlin.prototype.propertiesToVarArgs = function (props, type) {

  props = props || {};
  type = type || 'java.lang.Object';
  return this.java.newArray(type, _.flatten(_.forOwn(props, function (value, key) {
    if (key === 'label')
      key = this.T.label;
    return [key, value];
  }, this)));
};

