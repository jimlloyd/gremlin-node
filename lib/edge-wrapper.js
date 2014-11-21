'use strict';

var util = require('util');
var ElementWrapper = require('./element-wrapper');
var Q = require('q');

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

EdgeWrapper.prototype.jsonStringifySync = function (options) {
  // Return a json-formatted string (synchronously) for this edge.
  var self = this;
  var stream = new self.gremlin.ByteArrayOutputStream();
  var builder = self.gremlin.java.callStaticMethodSync('com.tinkerpop.gremlin.structure.io.graphson.GraphSONWriter', 'build');
  var writer = builder.createSync();
  writer.writeEdgeSync(stream, self.unwrap());
  return stream.toStringSync(self.gremlin.UTF8);
};

