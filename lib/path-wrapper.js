'use strict';

var assert = require('assert');
var Q = require('q');

var PathWrapper = module.exports = function (gremlin, path) {
  assert(path);
  assert(gremlin.isType(path, 'com.tinkerpop.gremlin.process.Path'));
  this.gremlin = gremlin;
  this.path = path;
};

// Return a promise to an array containing the objects in this Path.
PathWrapper.prototype.objects = function (callback) {
  var self = this;
  var objects = Q.nbind(this.path.objects, this.path);
  return objects()
    .then(function (list) {
      return self.gremlin._jsify(list);
    })
    .nodeify(callback);
};

// Return an array containing the objects in this Path.
PathWrapper.prototype.objectsSync = function (callback) {
  return this.gremlin._jsify(this.path.objectsSync());
};

// TODO: Rest of Path API
