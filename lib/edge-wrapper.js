'use strict';

var util = require('util');
var ElementWrapper = require('./element-wrapper');
var Q = require('q');
var _ = require('lodash');

var EdgeWrapper = module.exports = function (gremlin, el) {
  ElementWrapper.call(this, gremlin, el);
};

util.inherits(EdgeWrapper, ElementWrapper);

// public Vertex getVertex(Direction direction) throws IllegalArgumentException;

EdgeWrapper.prototype.getLabel = function () {
  return this.el.labelSync();
};

EdgeWrapper.prototype.setProperty = function (key, value, callback) {
  return Q.nbind(this.el.property, this.el)(key, value).nodeify(callback);
};

EdgeWrapper.prototype.jsonStringifySync = function () {
  // Return a json-formatted string (synchronously) for this edge.
  var stream = new this.gremlin.ByteArrayOutputStream();
  var builder = this.gremlin.java.callStaticMethodSync('com.tinkerpop.gremlin.structure.io.graphson.GraphSONWriter', 'build');
  var writer = builder.createSync();
  writer.writeEdgeSync(stream, this.unwrap());
  return stream.toStringSync(this.gremlin.UTF8);
};

EdgeWrapper.prototype.toJSON = function () {
  return JSON.parse(this.jsonStringifySync());
};
