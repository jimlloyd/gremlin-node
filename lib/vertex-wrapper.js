'use strict';

var util = require('util');
var ElementWrapper = require('./element-wrapper');
var Q = require('q');
var _ = require('lodash');
var assert = require('assert');   // TODO: remove

var VertexWrapper = module.exports = function (gremlin, el) {
  ElementWrapper.call(this, gremlin, el);
};

util.inherits(VertexWrapper, ElementWrapper);

// public Iterable<Edge> getEdges(Direction direction, String... labels);
// public Iterable<Vertex> getVertices(Direction direction, String... labels);
// public VertexQuery query();
// public Edge addEdge(String label, Vertex inVertex);

VertexWrapper.prototype.addEdge = function (label, inVertex, properties, callback) {
  var self = this;
  assert(_.isObject(properties));
  var propArray = _.flatten(_.pairs(properties));
  var arrayList = self.gremlin.java.newArray('java.lang.String', propArray);
  assert(inVertex instanceof VertexWrapper);
  return Q.nbind(self.el.addEdge, self.el)(label, inVertex.unwrap(), propArray)
    .then(function (e) { return self.gremlin.wrapEdge(e); })
    .nodeify(callback);
};

// VertexWrapper.prototype.singleProperty = function (properties, callback) {
//   assert(_.isObject(properties));
//
//   var keys = _.keys(properties);
//   assert(keys.length > 0);
//
//   var key1 = keys.pop();
//   var val1 = properties[key1].toString();
//
//   var rest = [];
//   _.reduce(keys, function (accumulator, key) {
//     accumulator.push(key);
//     accumulator.push(properties[key]);
//     return accumulator;
//   }, rest);
//   var arrayList = this.gremlin.java.newArray('java.lang.String', rest);
//
//   console.log(key1, val1, rest);
//
//   return Q.nbind(this.el.singleProperty, this.el)(key1, val1, arrayList)
//     .nodeify(callback);
// };

VertexWrapper.prototype.setProperty = function (key, value, callback) {
  return Q.nbind(this.el.singleProperty, this.el)(key, value, this.gremlin.emptyArrayList).nodeify(callback);
};

VertexWrapper.prototype.toJSON = function (callback) {
  var self = this;
  var stream = new self.gremlin.ByteArrayOutputStream();
  var builder = self.gremlin.java.callStaticMethodSync('com.tinkerpop.gremlin.structure.io.graphson.GraphSONWriter', 'build');
  var writer = builder.createSync();
  return Q.nbind(writer.writeVertex, writer)(stream, self.unwrap())
    .then(function () {
      return Q.nbind(stream.toString, stream)(self.gremlin.UTF8);
    })
    .nodeify(callback);
};
