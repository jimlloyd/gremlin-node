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
var IteratorWrapper = require('./iterator-wrapper');

var Gremlin = module.exports = function (opts) {
  opts = opts || {};
  opts.options = opts.options || [];
  opts.classpath = opts.classpath || [];

  // Add our own JAR first, so that we can provide alternate implementation of Gremlin classes for debugging.
  opts.classpath.push(path.join(__dirname, '..', 'target', 'gremlin-node-*.jar'));
  // Add the rest of the JAR's from the Maven package.
  opts.classpath.push(path.join(__dirname, '..', 'target', '*', '**', '*.jar'));

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
  this.HashSet = java.import('java.util.HashSet');
//   this.Table = java.import('com.tinkerpop.pipes.util.structures.Table');  // No TK3 equivalent?
//   this.Tree = java.import('com.tinkerpop.pipes.util.structures.Tree');   // TK2
  this.Tree = java.import('com.tinkerpop.gremlin.process.graph.util.Tree');   // TK3, but not used directly

  // This list of allowed function types should mirror the contents of the com.entrendipity.gremlinnode.function
  // package contained in this repo.
  var groovyFunction = 'com.entrendipity.gremlinnode.function.';
  this.GroovyLambda = java.import(groovyFunction + 'GroovyLambda');

  this.ScriptEngineLambda = java.import('com.tinkerpop.gremlin.process.computer.util.ScriptEngineLambda');
  this._groovyScriptEngineName = 'Groovy';
  this._javaScriptEngineName = 'JavaScript';

  this.T = java.import('com.tinkerpop.gremlin.process.T');
  this.Direction = java.import('com.tinkerpop.gremlin.structure.Direction');
  this.Compare = java.import('com.tinkerpop.gremlin.structure.Compare');
  this.Contains = java.import('com.tinkerpop.gremlin.structure.Contains');
  this.ByteArrayOutputStream = java.import('java.io.ByteArrayOutputStream');
  this.UTF8 = java.import('java.nio.charset.StandardCharsets').UTF_8.nameSync();
  this.createGraphSONWriter = java.callStaticMethodSync('com.tinkerpop.gremlin.structure.io.graphson.GraphSONWriter', 'build').createSync;

  this.emptyArrayList = java.newArray('java.lang.String', []);

  var AnonymousGraphTraversal_Tokens
      = java.import('com.tinkerpop.gremlin.process.graph.AnonymousGraphTraversal$Tokens');
  this.__ = this.wrapTraversal(AnonymousGraphTraversal_Tokens.__);

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

// Various TP3 methods such as Graph.addVertex, Vertex.addEdge, and Traversal.addE take
// a final argument Object... propertyKeyValues. For Javacript it is convenient to pass
// a simple Javascript object { key1: value1, key2: value2, ... }.
// This function converts such an object to the java Object[] array.
// *props* is the Javascript object.
// *type* is a java class path, which defaults to java.lang.Object.
Gremlin.prototype.propertiesToVarArgs = function (props, type) {
  props = props || {};
  type = type || 'java.lang.Object';
  return this.java.newArray(type, _.flatten(_.sortBy(_.pairs(props), function (x) { return x[0]; })));
};

// Applies *process* to each item returned by the *javaIterator*.
// *javaIterator* may be a java.util.iterator, or an IteratorWrapper, or a TraversalWrapper,
// or any type that type that implements hasNext() and next() as methods returning promises,
// with the semantics of those methods of java.util.iterator.
// *process* is function(item) {}, and may be either a synchronous function or async function returning a promise.
// Each item is processed completely before the next item is retrieved.
// Returns a promise if callback is omitted, else calls callback asynchronously when all items processed.
Gremlin.prototype.forEach = function (javaIterator, process, callback) {
  function _eachIterator(javaIterator, promisedProcess) {
    return javaIterator.hasNext()
      .then(function (hasNext) {
        if (!hasNext)
          return null;
        return promisedProcess(javaIterator.next())
          .then(function () {
            return _eachIterator(javaIterator, promisedProcess);
          });
      });
  }

  if (_.isUndefined(javaIterator.unwrap)) {
    if (this.java.instanceOf(javaIterator, 'java.util.Iterator'))
      javaIterator = new IteratorWrapper(javaIterator);
  }

  return _eachIterator(javaIterator, Q.promised(process)).nodeify(callback);
};

// Generates a general purpose Groovy lambda that can be used anywhere lambdas are accepted (e.g. filter, map, choose,
// etc.)
Gremlin.prototype.newGroovyLambda = function (groovy) {
  return new this.GroovyLambda(groovy, this.getEngine());
};

// Import a Java class or package into the Groovy engine.
// - *javaClassOrPkg* can be either class name, e.g. 'java.util.HashSet', or package spec, e.g. 'java.util.*'.
Gremlin.prototype.importGroovy = function (javaClassOrPkg) {
  var engine = this.getEngine();
  var imports = new this.HashSet();
  imports.addSync('import ' + javaClassOrPkg);
  engine.addImportsSync(imports);
};

// Generates a general purpose JavaScript lambda that can be used anywhere lambdas are accepted (e.g. filter, map,
// choose, etc.)
Gremlin.prototype.newJavaScriptLambda = function (javascript) {
  return new this.ScriptEngineLambda(this._javaScriptEngineName, javascript);
};
